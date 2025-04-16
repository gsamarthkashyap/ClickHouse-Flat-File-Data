from fastapi import FastAPI, UploadFile, File, Body, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from auth import get_current_user
import jwt
import csv
import os
from typing import List
import pandas as pd
import clickhouse_connect
from datetime import datetime, timedelta
from starlette.middleware.cors import CORSMiddleware # Import CORS middleware

# Secret key for JWT encoding/decoding
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # Token expiration time

# OAuth2PasswordBearer to handle token extraction from Authorization header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allow requests from your frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allow all headers (including Authorization)
)

# Connect to ClickHouse client
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='zeotap123',
    database='default',
)

@app.post("/setup-sample-table")
async def setup_sample_table(current_user: dict = Depends(get_current_user)):
    try:
        client.command("""
            CREATE TABLE IF NOT EXISTS products (
                id UInt32,
                name String,
                price Float32,
                in_stock UInt8
            ) ENGINE = MergeTree()
            ORDER BY id
        """)

        client.command("""
            INSERT INTO products (id, name, price, in_stock) VALUES
            (1, 'Apple', 0.5, 1),
            (2, 'Banana', 0.3, 1),
            (3, 'Cherry', 1.0, 0)
        """)
        return {"status": "success", "message": "Sample table created and data inserted"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ✅ Utility Function to Create Access Token
def create_access_token(data: dict, expires_delta: timedelta = None):
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = data.copy()
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# ✅ Utility Function to Verify Token
def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Token is invalid")

# ✅ NEW: Token Endpoint (Login Simulation)
@app.post("/token")
async def login_for_access_token():
    # Normally, you would validate username/password here
    user_data = {"sub": "user_example"}  # Example user data
    access_token = create_access_token(data=user_data)
    return {"access_token": access_token, "token_type": "bearer"}

# ✅ Protecting Routes with JWT Authentication
def get_current_user(token: str = Depends(oauth2_scheme)):
    return verify_token(token)

# ✅ Already Working (connection test)
@app.get("/connect/clickhouse")
async def connect_clickhouse(current_user: dict = Depends(get_current_user)):
    try:
        tables = client.query('SHOW TABLES')
        return {"status": "success", "tables": tables.result_set}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ✅ NEW: Get columns of a table
@app.get("/clickhouse/{table_name}/columns")
async def get_table_columns(table_name: str, current_user: dict = Depends(get_current_user)):
    try:
        result = client.query(f'DESCRIBE TABLE {table_name}')
        columns = [row[0] for row in result.result_set]  # Get column names only
        return {"status": "success", "columns": columns}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ✅ NEW: Export data from ClickHouse to CSV
@app.post("/ingest/clickhouse-to-flatfile")
async def ingest_clickhouse_to_flatfile(
    table_name: str = Body(..., embed=True),
    columns: List[str] = Body(..., embed=True),
    current_user: dict = Depends(get_current_user)
):
    try:
        # Construct the query to select the specified columns
        query = f"SELECT {', '.join(columns)} FROM {table_name}"
        
        # Query the data from ClickHouse
        result = client.query(query)
        
        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(result.result_set, columns=columns)
        
        # Save the data to a CSV file
        df.to_csv('output.csv', index=False)
        
        # Return the record count
        return {"status": "success", "record_count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ✅ Upload CSV and display columns
@app.post("/upload/csv")
async def upload_csv(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    # Save the uploaded file temporarily
    file_location = f"temp_{file.filename}"
    with open(file_location, "wb") as buffer:
        buffer.write(await file.read())
    
    # Read the CSV file columns
    columns = read_csv_columns(file_location)
    
    return JSONResponse(content={"columns": columns})

# Function to read CSV columns
def read_csv_columns(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        columns = reader.fieldnames  # Get column names from CSV
        return columns

PRODUCT_CSV_PATH = "C:/Users/gsama/Desktop/Projects/zeotap-ingestion-tool/backend/product.csv"

# ✅ Append uploaded CSV rows to the product.csv
@app.post("/append-to-product-csv")
async def append_to_product_csv(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    # Save uploaded file temporarily
    temp_file_path = f"temp_{file.filename}"
    with open(temp_file_path, "wb") as buffer:
        buffer.write(await file.read())
    
    # Log file paths for debugging
    print(f"PRODUCT_CSV_PATH: {PRODUCT_CSV_PATH}")
    print(f"Temporary file path: {temp_file_path}")
    
    # Read headers of uploaded file
    with open(temp_file_path, "r") as uploaded_file:
        reader = csv.DictReader(uploaded_file)
        uploaded_headers = reader.fieldnames
        uploaded_rows = list(reader)

    # Check if product.csv exists
    if not os.path.exists(PRODUCT_CSV_PATH):
        return JSONResponse(content={"status": "error", "message": "product.csv does not exist!"}, status_code=400)

    # Read headers from existing product.csv
    with open(PRODUCT_CSV_PATH, "r") as product_file:
        product_reader = csv.DictReader(product_file)
        product_headers = product_reader.fieldnames

    # Compare headers
    if uploaded_headers != product_headers:
        return JSONResponse(content={ 
            "status": "error", 
            "message": "Uploaded file's columns do not match product.csv"
        }, status_code=400)

    # Append rows to product.csv
    with open(PRODUCT_CSV_PATH, "a", newline='') as product_file:
        writer = csv.DictWriter(product_file, fieldnames=product_headers)
        writer.writerows(uploaded_rows)

    return JSONResponse(content={
        "status": "success",
        "message": f"Appended {len(uploaded_rows)} rows to product.csv"
    })

@app.post("/ingest/flatfile-to-clickhouse")
async def ingest_flatfile_to_clickhouse(
    file: UploadFile = File(...),
    table_name: str = Body(..., embed=True),
    columns: List[str] = Body(..., embed=True),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not file:
            raise HTTPException(status_code=400, detail="No upload file sent")
        if not table_name:
            raise HTTPException(status_code=400, detail="Target table name is required")
        if not columns or not isinstance(columns, list) or len(columns) == 0:
            raise HTTPException(status_code=400, detail="List of columns to ingest is required")

        contents = await file.read()
        # Decode bytes to string, handling potential encoding issues
        decoded_contents = contents.decode('utf-8', errors='ignore').splitlines()
        reader = csv.DictReader(decoded_contents)
        csv_columns = reader.fieldnames
        if not all(col in csv_columns for col in columns):
            raise HTTPException(
                status_code=400,
                detail=f"Selected columns not found in CSV file. Available columns: {csv_columns}"
            )

        data_to_insert = []
        for row in reader:
            row_values = [row.get(col) for col in columns]
            data_to_insert.append(row_values)

        if not data_to_insert:
            return {"status": "success", "ingested_rows": 0, "message": "No data found in the CSV to ingest for the selected columns."}

        # Check if the table exists. If not, you might want to create it
        # (This is a more advanced feature, for now, assume the table exists)
        try:
            client.query(f"SELECT 1 FROM {table_name} LIMIT 1")
        except clickhouse_connect.errors.ClickHouseError:
            raise HTTPException(
                status_code=400,
                detail=f"Table '{table_name}' does not exist in ClickHouse."
            )

        # Construct the INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Execute the insert query
        client.command(insert_query, data_to_insert)

        return {"status": "success", "ingested_rows": len(data_to_insert), "message": f"Successfully ingested {len(data_to_insert)} rows into '{table_name}'."}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))