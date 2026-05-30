from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse
from minio import Minio
from minio.error import S3Error
import os
import shutil
import zipfile
import io

app = FastAPI(title="MinIO Upload API")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"
DEFAULT_BUCKET   = os.getenv("MINIO_BUCKET", "nca-toolkit")
N8N_INPUT_FOLDER = os.getenv("N8N_INPUT_FOLDER", "/home/node/.n8n-files")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


def ensure_bucket(bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def delete_all_versions(bucket: str, object_name: str):
    """Supprime toutes les versions d'un objet (versioning activé)"""
    versions = list(client.list_objects(bucket, prefix=object_name, include_version=True))
    for v in versions:
        client.remove_object(bucket, v.object_name, version_id=v.version_id)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    bucket: str = Query(default=DEFAULT_BUCKET),
):
    try:
        ensure_bucket(bucket)
        original_name = file.filename
        folder_name = os.path.splitext(original_name)[0]
        object_name = f"{folder_name}/{original_name}"

        tmp_path = f"/tmp/{original_name}"
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        file_size = os.path.getsize(tmp_path)

        client.fput_object(
            bucket, object_name, tmp_path,
            content_type=file.content_type or "application/octet-stream",
        )
        os.remove(tmp_path)

        return {
            "status": "ok",
            "bucket": bucket,
            "folder": folder_name,
            "object": object_name,
            "original_filename": original_name,
            "size_bytes": file_size,
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur MinIO : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/transcription")
def get_transcription(
    folder_name: str = Query(...),
    bucket: str = Query(default=DEFAULT_BUCKET),
):
    try:
        objects = list(client.list_objects(bucket, prefix=f"{folder_name}/", recursive=True))
        transcription_files = [
            o for o in objects
            if o.object_name.endswith(".txt") or o.object_name.endswith(".json")
        ]
        if not transcription_files:
            raise HTTPException(status_code=404, detail=f"Aucune transcription trouvée dans '{bucket}/{folder_name}/'")

        obj = transcription_files[0]
        response = client.get_object(bucket, obj.object_name)
        content = response.read().decode("utf-8")

        return {"status": "ok", "bucket": bucket, "folder": folder_name, "file": obj.object_name, "content": content}

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/list")
def list_files(bucket: str = Query(default=DEFAULT_BUCKET), prefix: str = Query(default="")):
    try:
        ensure_bucket(bucket)
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        return {
            "bucket": bucket,
            "files": [{"name": obj.object_name, "size": obj.size, "last_modified": str(obj.last_modified)} for obj in objects],
        }
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete")
def delete_file(bucket: str = Query(...), object_name: str = Query(...)):
    try:
        delete_all_versions(bucket, object_name)
        return {"status": "ok", "deleted": object_name}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/cleanup")
def cleanup_folder(
    folder: str = Query(...),
    bucket: str = Query(default=DEFAULT_BUCKET),
):
    """Supprime tous les fichiers d'un dossier incluant toutes les versions"""
    try:
        # Tous les objets dans le dossier avec toutes leurs versions
        all_versions = list(client.list_objects(bucket, prefix=f"{folder}/", recursive=True, include_version=True))

        # Objet fantôme du dossier lui-même (ex: "mavideo/" ou "mavideo")
        root_versions = list(client.list_objects(bucket, prefix=folder, recursive=False, include_version=True))
        phantom = [o for o in root_versions if o.object_name in (f"{folder}/", folder)]
        all_versions += phantom

        if not all_versions:
            return {"status": "ok", "deleted": [], "message": "Dossier vide ou inexistant"}

        deleted = []
        for obj in all_versions:
            client.remove_object(bucket, obj.object_name, version_id=obj.version_id)
            deleted.append(obj.object_name)

        return {"status": "ok", "folder": folder, "deleted": deleted, "count": len(deleted)}

    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/cleanup-job")
def cleanup_job(
    job_id: str = Query(...),
    bucket: str = Query(default=DEFAULT_BUCKET),
):
    """Supprime les fichiers temporaires NCA Toolkit à la racine (par job_id)"""
    try:
        all_versions = list(client.list_objects(bucket, recursive=False, include_version=True))
        deleted = []
        for obj in all_versions:
            if obj.object_name.startswith(job_id):
                client.remove_object(bucket, obj.object_name, version_id=obj.version_id)
                deleted.append(obj.object_name)

        return {"status": "ok", "job_id": job_id, "deleted": deleted, "count": len(deleted)}

    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/download-folder")
def download_folder(
    folder: str = Query(...),
    bucket: str = Query(default=DEFAULT_BUCKET),
):
    """Retourne un zip de tous les fichiers d'un dossier"""
    try:
        objects = list(client.list_objects(bucket, prefix=f"{folder}/", recursive=True))
        if not objects:
            raise HTTPException(status_code=404, detail=f"Dossier '{folder}' vide ou inexistant")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for obj in objects:
                response = client.get_object(bucket, obj.object_name)
                filename = obj.object_name.split("/")[-1]
                zf.writestr(filename, response.read())

        zip_buffer.seek(0)
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={folder}.zip"},
        )

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))