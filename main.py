
from fastapi import FastAPI, Request, Form, UploadFile, File, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
import pandas as pd
from google.cloud import storage
from pandas_gbq import to_gbq
import os
from typing import Optional


app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key='your-secret-key')
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Dummy user for login
dummy_user: dict[str, str] = {"username": "admin", "password": "password"}

# Google Cloud Storage and BigQuery config
GCS_BUCKET: str = "your-bucket-name"
BQ_PROJECT: str = "your-gcp-project"
BQ_DATASET: str = "your_dataset"
BQ_TABLE: str = "your_table"

def get_current_user(request: Request) -> str:
    """
    Dependency to get the current logged-in user from the session.
    Raises HTTPException if not authenticated.
    """
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


@app.get("/", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    """
    Render the login page.
    """
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
) -> HTMLResponse:
    """
    Handle login form submission. Sets session if credentials are correct.
    """
    if username == dummy_user["username"] and password == dummy_user["password"]:
        request.session["user"] = username
        return RedirectResponse("/upload", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})


@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, user: str = Depends(get_current_user)) -> HTMLResponse:
    """
    Render the upload page. Requires authentication.
    """
    return templates.TemplateResponse("upload.html", {"request": request})


@app.post("/upload")
def upload_csv(
    request: Request,
    file: UploadFile = File(...),
    user: str = Depends(get_current_user)
) -> HTMLResponse:
    """
    Handle CSV upload, save to GCS, and load into BigQuery.
    """
    # Save to temp file
    temp_path: str = f"/tmp/{file.filename}"
    with open(temp_path, "wb") as f:
        f.write(file.file.read())
    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(file.filename)
    blob.upload_from_filename(temp_path)
    # Load to BigQuery
    df: pd.DataFrame = pd.read_csv(temp_path)
    to_gbq(df, f"{BQ_DATASET}.{BQ_TABLE}", project_id=BQ_PROJECT, if_exists="replace")
    os.remove(temp_path)
    return templates.TemplateResponse("upload.html", {"request": request, "success": "File uploaded and loaded to BigQuery!"})
