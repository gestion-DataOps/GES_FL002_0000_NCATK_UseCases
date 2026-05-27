from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from minio import Minio
from minio.error import S3Error
import os
import shutil
import uuid

app = FastAPI(title="MinIO Upload API")

# Configuration via variables d'environnement
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "nca-toolkit")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


def ensure_bucket(bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    bucket: str = Query(default=DEFAULT_BUCKET),
    folder: str = Query(default=""),
):
    try:
        ensure_bucket(bucket)

        # Nom unique pour éviter les collisions
        ext = os.path.splitext(file.filename)[1]
        unique_name = f"{uuid.uuid4()}{ext}"
        object_name = f"{folder}/{unique_name}" if folder else unique_name

        # Sauvegarde temporaire
        tmp_path = f"/tmp/{unique_name}"
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        file_size = os.path.getsize(tmp_path)

        # Upload avec multipart automatique (géré par le SDK MinIO)
        client.fput_object(
            bucket,
            object_name,
            tmp_path,
            content_type=file.content_type or "application/octet-stream",
        )

        os.remove(tmp_path)

        return {
            "status": "ok",
            "bucket": bucket,
            "object": object_name,
            "original_filename": file.filename,
            "size_bytes": file_size,
            "url": f"/{bucket}/{object_name}",
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur MinIO : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete")
def delete_file(bucket: str = Query(...), object_name: str = Query(...)):
    try:
        client.remove_object(bucket, object_name)
        return {"status": "ok", "deleted": object_name}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/list")
def list_files(bucket: str = Query(default=DEFAULT_BUCKET), prefix: str = Query(default="")):
    try:
        ensure_bucket(bucket)
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        return {
            "bucket": bucket,
            "files": [
                {
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": str(obj.last_modified),
                }
                for obj in objects
            ],
        }
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))