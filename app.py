import streamlit as st # test 
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from fpdf import FPDF
import gspread
#from googletrans import Translator
from oauth2client.service_account import ServiceAccountCredentials
import base64
import time
import os
import re
from io import StringIO
import hashlib
import secrets
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import io  # For in-memory file handling
from urllib.parse import quote_plus

from datetime import datetime, date 
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.textlabels import Label
from reportlab.lib.units import inch


# Function to load and process data
@st.cache_data
def load_and_process_data(file):
    df = pd.read_csv(file)
    df = df.drop(columns=["unnecessary_column"])
    return df

@st.cache_resource
def get_gspread_client():
    """Establishes and caches the connection to Google Sheets for performance."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": st.secrets["connections"]["gsheets"]["type"],
        "project_id": st.secrets["connections"]["gsheets"]["project_id"],
        "private_key_id": st.secrets["connections"]["gsheets"]["private_key_id"],
        "private_key": st.secrets["connections"]["gsheets"]["private_key"],
        "client_email": st.secrets["connections"]["gsheets"]["client_email"],
        "client_id": st.secrets["connections"]["gsheets"]["client_id"],
        "auth_uri": st.secrets["connections"]["gsheets"]["auth_uri"],
        "token_uri": st.secrets["connections"]["gsheets"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["connections"]["gsheets"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["connections"]["gsheets"]["client_x509_cert_url"]
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

# Function to connect to Google Sheets
def connect_to_google_sheet(sheet_name):
    client = get_gspread_client()
    sheet = client.open(sheet_name).sheet1
    return sheet

# Function to hash passwords with a salt for better security
def hash_password(password, salt):
    return hashlib.sha256(salt.encode() + password.encode()).hexdigest()

# Function to create a new user account in Google Sheet
def create_user_account(school_id, password, email, school_name, logo_file):
    try:
        sheet = connect_to_google_sheet("Apnapan User Accounts")
        all_school_ids = sheet.col_values(1)
        if school_id in all_school_ids:
            return False, "School ID already exists."

        # Generate a salt and hash the password
        salt = secrets.token_hex(16)
        hashed_password = hash_password(password, salt)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Handle logo upload
        logo_base64 = ""
        if logo_file:
            logo_bytes = logo_file.read()
            logo_base64 = base64.b64encode(logo_bytes).decode()  # Convert to base64 string

        # Append new user data including the school name and logo
        sheet.append_row([school_id, hashed_password, salt, email, school_name, logo_base64, timestamp])
        return True, "Account created successfully!"
    except Exception as e:
        return False, f"Error creating account: {str(e)}"

# Function to validate login credentials
def validate_login(school_id, password):
    """Validates user login using salted password hashes."""
    try:
        sheet = connect_to_google_sheet("Apnapan User Accounts")
        # Fetch all school IDs to perform a reliable check, avoiding data type issues with sheet.find()
        all_school_ids = sheet.col_values(1)
        
        if school_id in all_school_ids:
            # Find the row index (1-based) and fetch the corresponding user data
            row_index = all_school_ids.index(school_id) + 1
            user_data = sheet.row_values(row_index)
            # Assuming columns are: School ID (1), Password (2), Salt (3)
            stored_hash = user_data[1]
            salt = user_data[2]

            # Hash the provided password with the stored salt and compare
            hashed_input_password = hash_password(password, salt)
            if hashed_input_password == stored_hash:
                return True, "Login successful!"
            else:
                return False, "Invalid password."
        else:
            return False, "School ID not found."

    except Exception as e:
        return False, f"Error validating login: {str(e)}"

# Function to validate user for password reset
def validate_reset_request(school_id, email):
    """Checks if the school_id and email match a record."""
    try:
        sheet = connect_to_google_sheet("Apnapan User Accounts")
        all_school_ids = sheet.col_values(1)
        if school_id in all_school_ids:
            row_index = all_school_ids.index(school_id) + 1
            user_data = sheet.row_values(row_index)
            # Assuming columns: School ID (1), Password (2), Salt (3), Email (4)
            stored_email = user_data[3]
            if email.strip().lower() == stored_email.strip().lower():
                return True, "Verification successful. Please set your new password."
            else:
                return False, "The email address provided does not match our records for this School ID."
        else:
            return False, "School ID not found."
    except Exception as e:
        return False, f"An error occurred during verification: {str(e)}"

# Function to update user password in the sheet
def update_user_password(school_id, new_password):
    """Finds a user by school_id and updates their password."""
    try:
        sheet = connect_to_google_sheet("Apnapan User Accounts")
        all_school_ids = sheet.col_values(1)
        if school_id in all_school_ids:
            row_index = all_school_ids.index(school_id) + 1
            user_data = sheet.row_values(row_index)
            # Assuming Salt is in column 3
            salt = user_data[2]
            new_hashed_password = hash_password(new_password, salt)
            sheet.update_cell(row_index, 2, new_hashed_password)  # Update password in column 2
            return True, "Password has been updated successfully!"
        else:
            # This case should ideally not be hit if the flow is correct
            return False, "School ID not found. Could not update password."
    except Exception as e:
        return False, f"An error occurred while updating password: {str(e)}"
    

@st.cache_data(ttl=3600)  # Cache for 1 hour to reduce API calls
def get_school_details(school_id):
    try:
        sheet = connect_to_google_sheet("Apnapan User Accounts")
        all_school_ids = sheet.col_values(1)
        if school_id in all_school_ids:
            row_index = all_school_ids.index(school_id) + 1
            user_data = sheet.row_values(row_index)
            # Assuming columns: School ID (1), Password (2), Salt (3), Email (4), School Name (5), Logo (6)
            school_name = user_data[4]
            logo_base64 = user_data[5]
            return school_name, logo_base64
        else:
            return None, None
    except Exception as e:
        st.error(f"Error fetching school details: {str(e)}")
        return None, None


# Function to get MIME type for file download
def get_mime_type(filename):
    """Returns the MIME type based on the file extension."""
    ext = filename.split('.')[-1].lower()
    if ext == 'csv':
        return 'text/csv'
    elif ext == 'xlsx':
        return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif ext == 'xls':
        return 'application/vnd.ms-excel'
    elif ext == 'txt':
        return 'text/plain'
    return 'application/octet-stream'  # Generic fallback

# Function to connect to MongoDB collection
@st.cache_resource  # Cache for efficiency
def get_mongo_collection():
    # Encode username and password
    username = quote_plus(st.secrets["mongo"]["username"])
    password = quote_plus(st.secrets["mongo"]["password"])
    host = st.secrets["mongo"]["host"]
    db_name = st.secrets["mongo"]["db_name"]
    collection_name = st.secrets["mongo"]["collection_name"]

    # Construct the URI with encoded credentials
    uri = f"mongodb+srv://{username}:{password}@{host}/{db_name}?retryWrites=true&w=majority"
    
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

# Function to upload file to MongoDB (store as binary)
def upload_file_to_mongo(school_id, uploaded_file):
    collection = get_mongo_collection()
    try:
        file_data = uploaded_file.getvalue()  # Bytes
        timestamp = datetime.now()
        doc = {
            "school_id": school_id,
            "filename": uploaded_file.name,
            "file_data": file_data,  # Binary data
            "timestamp": timestamp
        }
        collection.insert_one(doc)
        return True
    except PyMongoError as e:
        st.error(f"Upload error: {e}")
        return False

# Function to list user's files from MongoDB
def list_user_files(school_id):
    collection = get_mongo_collection()
    try:
        # Use an aggregation pipeline to get the latest version of each unique filename
        pipeline = [
            {"$match": {"school_id": school_id}},  # Filter by school
            {"$sort": {"timestamp": -1}},  # Sort by date, newest first
            {
                "$group": {  # Group by filename
                    "_id": "$filename",
                    "latest_timestamp": {"$first": "$timestamp"}  # Get the newest timestamp
                }
            },
            {
                "$project": {  # Reshape the output
                    "_id": 0,
                    "filename": "$_id",
                    "timestamp": "$latest_timestamp"
                }
            },
            {"$sort": {"timestamp": -1}}  # Sort the final list by date
        ]
        files = list(collection.aggregate(pipeline))
        return files  # List of dicts: [{'filename': 'file.csv', 'timestamp': datetime}]
    except PyMongoError as e:
        st.error(f"Error listing files: {e}")
        return []

# Function to download file from MongoDB by filename (latest if duplicates)
def download_file_from_mongo(school_id, filename):
    collection = get_mongo_collection()
    try:
        file_doc = collection.find_one({"school_id": school_id, "filename": filename}, sort=[("timestamp", -1)])
        if file_doc:
            return io.BytesIO(file_doc["file_data"])  # Return BytesIO for processing
        else:
            st.error("File not found.")
            return None
    except PyMongoError as e:
        st.error(f"Download error: {e}")
        return None

# Set page config for mobile-friendly design
st.set_page_config(layout="wide", page_title="Data Insights Generator")

# Initialize session state for navigation
if 'current_page' not in st.session_state:
    st.session_state['current_page'] = 'login'
    
# Define the navigate_to function
def navigate_to(page):
    st.session_state['current_page'] = page
    
# Custom CSS for consistent theme and centering
st.markdown("""
    <style>
        .stApp {
            background-color: #d6ecf9;
            font-family: 'Segoe UI', sans-serif;
            color: black !important;
        }
        h1, h2, h3, h4 {
            color: #003366 !important;
        }
        .stTextInput > div > div > input {
            background-color: #ff6666 !important;
            color: white !important;
            border-radius: 20px !important;
            padding: 12px !important;
            border: none !important;
            font-size: 16px !important;
        }
        .stTextInput > div > div > input::placeholder {
            color: white !important;
            opacity: 0.8 !important;
        }
        .forgot-link {
            color: #ff6666 !important;
            font-size: 14px;
            text-decoration: none;
        }
        .pulse-text {
            text-align: center;
            font-size: 18px;
            color: #ff6666;
            margin-top: 20px;
        }
        .bubble {
            background-color: #ff9999;
            color: #003366;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            margin-left: 8px;
        }
        .stTextInput label, .stFileUploader label {
            color: black !important;
        }

        button, .stDownloadButton button {{
            background-color: #ff6666 !important;
            color: white !important;
            border-radius: 8px;


            
        }}

        /* Style for the history download button to match other buttons */
        .history-download-button .stDownloadButton button {
            background-color: #ff6666 !important; /* Match other buttons */
            color: white !important;
            font-weight: bold !important;
            border: 1px solid #e05252 !important; /* Darker red border for consistency */
            width: 100%; /* Make it full width to match other elements */
        }
        .history-download-button .stDownloadButton button:hover {
            background-color: #e05252 !important; /* Darker red on hover */
            border-color: #c44141 !important;
        }

        /* Ensure text inside all buttons is white */
        div[data-testid="stForm"] button p,
        div[data-testid="stButton"] > button p,
        div[data-testid="stDownloadButton"] button p {
            color: white !important;
        }

        /* Hover effect for all buttons */
        div[data-testid="stForm"] button:hover,
        div[data-testid="stButton"] > button:hover {
            background-color: #333333 !important; /* Slightly lighter black on hover */
        }
    </style>
""", unsafe_allow_html=True)

# Add global CSS for button styling to ensure compatibility with light and dark themes
st.markdown("""
    <style>
        /* General button styling for light and dark themes */
        button, .stButton > button, .stDownloadButton > button {
            background-color: #0a0504 !important; /* Primary button color */
            color: white !important; /* Text color */
            border-radius: 8px !important; /* Rounded corners */
            border: none !important; /* Remove border */
            font-weight: bold !important; /* Bold text */
        }
        button:hover, .stButton > button:hover, .stDownloadButton > button:hover {
            background-color: #e05252 !important; /* Darker shade on hover */
        }
        button:focus, .stButton > button:focus, .stDownloadButton > button:focus {
            outline: none !important; /* Remove focus outline */
            box-shadow: 0 0 4px 2px rgba(255, 102, 102, 0.5) !important; /* Add focus shadow */
        }
        /* Ensure text inside all buttons is visible in both themes */
        div[data-testid="stForm"] button p,
        div[data-testid="stButton"] > button p,
        div[data-testid="stDownloadButton"] button p {
            color: white !important; /* Ensure text is white */
        }
    </style>
""", unsafe_allow_html=True)

# Login Page
if st.session_state['current_page'] == 'login':
    # Load and encode logo
    logo_base64 = ""
    logo_path = "images/project_apnapan_logo.png"
    if os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as img_file:
                logo_base64 = base64.b64encode(img_file.read()).decode()
            print(f"Logo loaded successfully: {logo_path}, length: {len(logo_base64)}")
        except Exception as e:
            print(f"Error loading logo: {e}")
    else:
        print(f"Logo file not found: {logo_path}")

    # Display title and logo
    st.markdown("<h1 style='text-align: center; color: white;'>Apnapan Pulse</h1>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="display: flex; justify-content: center; margin-bottom: 30px;">
        <img src="data:image/png;base64,{logo_base64}" alt="Project Apnapan Logo" style="height: 100px;" />
    </div>
    """, unsafe_allow_html=True)

    # Login form container
    st.markdown("<div class='login-container'>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: black;'>Log in credentials</h3>", unsafe_allow_html=True)

    with st.form(key="login_form"):
    # Center the input fields using columns
        col1, col2, col3 = st.columns([1, 2, 1])  # Left padding, content, right padding
        with col2:  # Center the input fields
            school_id = st.text_input("School ID", placeholder="Enter your school ID", key="school_id")
            password = st.text_input("Password", placeholder="Enter your security pin", type="password", key="password")
            
            login_button = st.form_submit_button("Find your pulse!", use_container_width=True)

            # Use columns for the other two actions
            col_b1, col_b2 = st.columns(2)
            with col_b1:
                create_account_button = st.form_submit_button("Create Account", use_container_width=True, help="Click to create a new account")
            with col_b2:
                forgot_password_button = st.form_submit_button("Forgot Password?", use_container_width=True, help="Click to reset your password")

            if forgot_password_button:
                navigate_to('forgot_password')
                st.rerun()

        # Add custom CSS to reduce the size of the "Show Password" text
        st.markdown("""
        <style>
            label[for="password"] {
                font-size: 12px !important; /* Reduce font size */
                color: #666 !important; /* Optional: Change color */
            }
        </style>
        """, unsafe_allow_html=True)

        if login_button:
            success, message = validate_login(school_id, password)
            if success:
                st.session_state['logged_in_user'] = school_id  # Store user's ID
                st.success(message)
                # Fetch and store school details in session state to avoid repeated API calls
                with st.spinner("Loading school details..."):
                    school_name, school_logo_base64 = get_school_details(school_id)
                    st.session_state['school_name'] = school_name
                    st.session_state['school_logo_base64'] = school_logo_base64
                navigate_to('landing')
                st.rerun()
            else:
                st.error(message)

        if create_account_button:
            navigate_to('create_account')
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()
    
# Create Account Page
if st.session_state['current_page'] == 'create_account':
    st.title("Create Account")
    st.write("Fill in the details below to create a new account.")

    with st.form(key="create_account_form"):
        new_school_id = st.text_input("School ID", placeholder="Enter your school ID")
        new_password = st.text_input("Password", placeholder="Enter your password", type="password")
        confirm_password = st.text_input("Confirm Password", placeholder="Re-enter your password", type="password")
        email = st.text_input("Email", placeholder="Enter your email address")
        school_name = st.text_input("School Name", placeholder="Enter your school name")
        logo_file = st.file_uploader("Upload School Logo (Optional)", type=["png", "jpg", "jpeg"])

        submitted = st.form_submit_button("Create Account")
        if submitted:
            if new_password != confirm_password:
                st.error("Passwords do not match. Please try again.")
            elif not new_school_id or not new_password or not email or not school_name:
                st.error("All fields except the logo are required. Please fill in all the details.")
            else:
                success, message = create_user_account(new_school_id, new_password, email, school_name, logo_file)
                if success:
                    st.success(message)
                    navigate_to('login')
                    st.rerun()
                else:
                    st.error(message)

    if st.button("Back to Login", key="back_to_login_from_create"):
        navigate_to('login')
        st.rerun()

    st.stop()

# Forgot Password Page
if st.session_state['current_page'] == 'forgot_password':
    st.title("Reset Your Password")

    # Initialize state for the multi-step form
    if 'reset_step' not in st.session_state:
        st.session_state.reset_step = 1
    if 'reset_school_id' not in st.session_state:
        st.session_state.reset_school_id = None

    # Step 1: Verify User
    if st.session_state.reset_step == 1:
        st.write("Enter your School ID and registered email to verify your account.")
        with st.form(key="verify_user_form"):
            school_id = st.text_input("School ID", placeholder="Enter your school ID")
            email = st.text_input("Registered Email", placeholder="Enter the email you signed up with")
            
            submitted = st.form_submit_button("Verify Account")
            if submitted:
                if not school_id or not email:
                    st.error("Please enter both your School ID and email address.")
                else:
                    success, message = validate_reset_request(school_id, email)
                    if success:
                        st.session_state.reset_school_id = school_id
                        st.session_state.reset_step = 2
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)

    # Step 2: Set New Password
    elif st.session_state.reset_step == 2:
        st.write(f"Account verified for School ID: **{st.session_state.reset_school_id}**")
        st.write("You can now set a new password.")
        with st.form(key="set_new_password_form"):
            new_password = st.text_input("New Password", type="password")
            confirm_password = st.text_input("Confirm New Password", type="password")

            submitted = st.form_submit_button("Set New Password")
            if submitted:
                if not new_password or not confirm_password:
                    st.error("Please fill out both password fields.")
                elif new_password != confirm_password:
                    st.error("Passwords do not match. Please try again.")
                else:
                    success, message = update_user_password(st.session_state.reset_school_id, new_password)
                    if success:
                        st.success(message)
                        st.info("You can now log in with your new password.")
                        # Reset state and prepare for navigation
                        del st.session_state.reset_step
                        del st.session_state.reset_school_id
                    else:
                        st.error(message)

    # Always show the back to login button, but handle state reset
    if st.button("Back to Login", key="back_to_login_from_forgot"):
        # Clean up state if user navigates away mid-process
        if 'reset_step' in st.session_state:
            del st.session_state.reset_step
        if 'reset_school_id' in st.session_state:
            del st.session_state.reset_school_id
        navigate_to('login')
        st.rerun()

    st.stop()

# Initialize session state for navigation
if 'current_page' not in st.session_state:
    st.session_state['current_page'] = 'landing'

# Function to navigate to a specific page
def navigate_to(page):
    st.session_state['current_page'] = page

scale_base64 = ""
scale_path = "images/Likert_Scale.png"  # Make sure this matches your file name and location
if os.path.exists(scale_path):
    try:
        with open(scale_path, "rb") as img_file:
            scale_base64 = base64.b64encode(img_file.read()).decode()
    except Exception as e:
        print(f"Error loading Likert scale image: {e}")
else:
    print(f"Scale file not found: {scale_path}")


st.markdown("""
    <style>
    /* General fix for all checkbox labels */
    .stCheckbox div[data-testid="stMarkdownContainer"] > p {
        color: black !important;
        font-weight: 500;
    }
    /* Extra fallback for some Streamlit versions */
    .stCheckbox label {
        color: black !important;
    }
    </style>
""", unsafe_allow_html=True)

# Load and encode logo
logo_base64 = ""
logo_path = "images/project_apnapan_logo.png"  # Adjust this path to match your file location
if os.path.exists(logo_path):
    try:
        with open(logo_path, "rb") as img_file:
            logo_base64 = base64.b64encode(img_file.read()).decode()
        print(f"Logo loaded successfully: {logo_path}, length: {len(logo_base64)}")
    except Exception as e:
        print(f"Error loading logo: {e}")
    else:
        print(f"Logo file not found: {logo_path}")

# Inject CSS for styling
st.markdown("""
<style>
    div[data-testid="stVerticalBlock"] label {
        font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
    <style>
        .stApp {{
            background-color: #d6ecf9;
            font-family: 'Segoe UI', sans-serif;
            color: black !important;
        }}
        [data-testid="stSidebar"] > div:first-child {{
            background-color: #def2e3;
            border-radius: 10px;
            padding: 1rem;
            color: black !important;
        }}
        h1, h2, h3, h4 {{
            color: #003366 !important;
        }}
        .stMetric label, .stMetric span {{
            color: #003366 !important;
        }}
        .stFileUploader {{
            border: 2px dashed #3366cc;
            padding: 10px;
            background-color: #ffffff;
            border-radius: 10px;
            color: black;
        }}
        .custom-logo {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-top: -10px;
            margin-bottom: 20px;
        }}
        .custom-logo img {{
            height: 60px;
        }}
        .custom-logo span {{
            font-size: 26px;
            font-weight: bold;
            color: #003366;
        }}
        .block-container {{
            padding-top: 1.5rem;
        }}
        .stAlert p, .stAlert div, .stAlert {{
            color: black !important;
        }}
        .css-1cpxqw2, .css-ffhzg2 {{
            color: black !important;
        }}
        label, .stCheckbox > div, .stRadio > div, .stSelectbox > div,
        .stMultiSelect > div, .css-16idsys, .css-1r6slb0, .css-1n76uvr {{
            color: black !important;
        }}
""", unsafe_allow_html=True)

# Sample dataset for preview (move this up!)
sample_data = pd.DataFrame({
    "StudentID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "Gender": ["Male", "Female", "Male", "Female", "Male", "Female", "Male", "Female", "Male", "Female"],
    "Grade": [10, 9, 11, 8, 12, 10, 9, 11, 10, 12],
    "Religion": ["Hindu", "Muslim", "Christian", "Sikh", "Hindu", "Buddhist", "Jain", "Islam", "Hindu", "Christian"],
    "Ethnicity_cleaned": ["Asian", "African", "Latin", "Asian", "African", "Latin", "Asian", "African", "Latin", "Asian"],
    "What_items_among_these_do_you_have_at_home": [
        "Car, Computer, Apna Ghar", "Laptop, Rent", "Apna Ghar", "Computer", "Car, Apna Ghar",
        "Rent", "Computer, Apna Ghar", "Laptop", "Car, Computer", "Apna Ghar"
    ],
    "Do_you_feel_safe_at_school": ["Agree", "Neutral", "Strongly Agree", "Disagree", "Agree", "Neutral", "Strongly Agree", "Disagree", "Agree", "Neutral"],
    "Do_you_feel_welcome_at_school": ["Strongly Agree", "Agree", "Neutral", "Disagree", "Agree", "Neutral", "Strongly Agree", "Neutral", "Agree", "Disagree"],
    "Are_you_respected_by_peers": ["Neutral", "Agree", "Strongly Agree", "Neutral", "Agree", "Disagree", "Strongly Agree", "Neutral", "Agree", "Neutral"],
    "Do_teachers_notice_you": ["Disagree", "Neutral", "Agree", "Disagree", "Neutral", "Strongly Disagree", "Agree", "Disagree", "Neutral", "Disagree"],
    "Do_you_have_a_close_teacher": ["Agree", "Neutral", "Strongly Agree", "Disagree", "Agree", "Neutral", "Strongly Agree", "Disagree", "Agree", "Neutral"]
})


def process_data_and_calculate_metrics(df):
    """
    Takes a raw DataFrame, performs all cleaning, normalization, and metric calculations.
    This centralized function is key to the app's performance.
    """
    df_cleaned = df.copy()  # Work on a copy

    # Define mappings inside the function for encapsulation
    questionnaire_mapping = {
        "Strongly Disagree": 1, "Disagree": 2, "Neutral": 3, "Agree": 4, "Strongly Agree": 5
    }

    # --- General Demographic Data Normalization (Case-Insensitive) ---
    demographic_keywords = ["gender", "religion"]
    for col in df_cleaned.columns:
        if any(keyword in col.lower() for keyword in demographic_keywords):
            df_cleaned[col] = df_cleaned[col].astype(str).str.strip().str.title()
            df_cleaned[col] = df_cleaned[col].replace('Nan', 'Unknown')

    # --- Grade Column Normalization ---
    grade_column = next((col for col in df_cleaned.columns if "grade" in col.lower()), None)
    if grade_column:
        def normalize_grade(value):
            s_val = str(value).strip()
            numbers = re.findall(r'\d+', s_val)
            if numbers:
                return str(numbers[0])
            return s_val.title() if s_val.lower() not in ['nan', ''] else 'Unknown'
        df_cleaned[grade_column] = df_cleaned[grade_column].apply(normalize_grade)

    # --- Questionnaire Mapping (convert to numeric) ---
    questionnaire_cols = [
        col for col in df_cleaned.columns
        if any(str(val).strip().title() in questionnaire_mapping for val in df_cleaned[col].dropna())
    ]
    if questionnaire_cols:
        for col in questionnaire_cols:
            df_cleaned[col] = df_cleaned[col].astype(str).str.strip().str.title()
            df_cleaned[col] = df_cleaned[col].map(questionnaire_mapping).fillna(df_cleaned[col])
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce")

    # --- Improved, Case-Insensitive Ethnicity Cleaning ---
    ethnicity_column = next((col for col in df_cleaned.columns if "ethnicity" in col.lower()), None)
    if ethnicity_column:
        def clean_ethnicity(value):
            v_lower = str(value).lower().strip()
            if "general" in v_lower:
                return "General"
            if "sc" in v_lower:
                return "SC"
            if "other" in v_lower: # For OBC
                return "OBC"
            if "do" in v_lower: # For "Don't know"
                return "Don't Know"
            if "st" in v_lower:
                return "ST"
            return str(value).strip().title() # Default: clean and title-case unmatched values
        df_cleaned["ethnicity_cleaned"] = df_cleaned[ethnicity_column].apply(clean_ethnicity)

    # --- Define Belonging Constructs ---
    belonging_questions = {
        "Safety": ["safe", "surakshit"],
        "Respect": ["respected", "izzat", "as much respect"],
        "Welcome": ["being welcomed", "welcome", "swagat"],
        "Relationships with Teachers": ["one teacher", "share your problem", "care about your feelings", " care about how I feel", "feel close", "close to your teachers"],
        "Participation": ["opportunities", "participate", "school activities", "take part", "join in many activities"],
        "Acknowledgement": ["notice", "noticed", "listen to you", "dekhein", "acknowledge", "recognized", "listen to what I say", "valued", "heard", "seen", "like you", "like me", "do something well"]
    }

    # --- Match Constructs to Question Columns ---
    matched_questions = {
        cat: [col for col in df_cleaned.columns if any(k.lower() in col.lower() for k in keywords)]
        for cat, keywords in belonging_questions.items()
    }

    # --- Special Handling: "Kaash" Questions ---
    kaash_col = [
        col for col in df_cleaned.columns if "kaash" in col.lower()]
    df_cleaned["KaashScore"] = (
        df_cleaned[kaash_col].apply(pd.to_numeric, errors="coerce").mean(axis=1) if kaash_col else 0
    )

    # --- Compute Belonging Scores ---
    belonging_cols = [col for sublist in matched_questions.values() for col in sublist]
    if belonging_cols:
        df_cleaned["BelongingRaw"] = df_cleaned[belonging_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        df_cleaned["BelongingCount"] = df_cleaned[belonging_cols].apply(pd.to_numeric, errors="coerce").notna().sum(axis=1)
        df_cleaned["BelongingScore"] = df_cleaned.apply(
            lambda row: (row["BelongingRaw"] - row["KaashScore"]) / row["BelongingCount"] if row["BelongingCount"] > 0 else 0,
            axis=1
        )
    else:
        df_cleaned["BelongingRaw"] = 0
        df_cleaned["BelongingCount"] = 0
        df_cleaned["BelongingScore"] = 0

    # --- Aggregate Insights ---
    overall_belonging_score = df_cleaned["BelongingScore"].mean() if belonging_cols else None
    category_averages = {
        cat: df_cleaned[cols].apply(pd.to_numeric, errors='coerce').mean().mean() if cols else 0
        for cat, cols in matched_questions.items()
    }
    highest_area = max(category_averages, key=category_averages.get) if category_averages else None
    valid_categories = {k: v for k, v in category_averages.items() if v > 0.00}
    lowest_area = min(valid_categories, key=valid_categories.get) if valid_categories else None

    # --- Package results into a dictionary for clean state management ---
    results = {
        'df_cleaned': df_cleaned,
        'matched_questions': matched_questions,
        'belonging_questions': belonging_questions,
        'overall_belonging_score': overall_belonging_score,
        'category_averages': category_averages,
        'highest_area': highest_area,
        'lowest_area': lowest_area,
        'matched_questions_table': pd.DataFrame.from_dict(matched_questions, orient="index").T.fillna("")
    }
    return results

# Landing Page
if st.session_state['current_page'] == 'landing':
    # Header with Project Apnapan logo and school details on the same line
    col1, col2 = st.columns([4, 4])  # Adjust column widths for alignment

    with col1:
        # Project Apnapan logo and name
        st.markdown(f"""
            <div class="custom-logo">
                <img src="data:image/png;base64,{logo_base64}" alt="Project Apnapan Logo" />
                <span>Project Apnapan</span>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        # School logo and name
        if 'logged_in_user' in st.session_state:
            school_name = st.session_state.get('school_name')
            school_logo_base64 = st.session_state.get('school_logo_base64')

            school_logo_html = ""
            if school_logo_base64:
                school_logo_html = f'<img src="data:image/png;base64,{school_logo_base64}" alt="School Logo" style="height: 50px;" />'

            school_name_html = ""
            if school_name:
                school_name_html = f'<h4 style="margin: 0; color: #003366 !important;">{school_name}</h4>'

            if school_logo_html or school_name_html:
                st.markdown(f"""
                    <div style="display: flex; justify-content: flex-end; align-items: center; gap: 12px; padding-top: 10px;">
                        {school_logo_html}
                        {school_name_html}
                    </div>
                """, unsafe_allow_html=True)

    st.title("Welcome to the Data Insights Generator!")
    st.write("Your journey to understanding students’ experiences begins here.")
    st.write("This easy-to-use tool is designed to help schools uncover meaningful insights about student belonging and well-being. Let’s get started!")
    
    st.markdown(
    """
    <div style="font-size:1.15rem;">
        <div style="margin-bottom: 10px;">
            <span style="font-size:1.25rem; font-weight:700; color:#003366;">Step-1:</span>
            <span style="font-weight:600;">Upload Your Data</span>
            <br>
            <span>
                Click on the
                <span style="color:#d7263d; font-weight:bold; background:#fff3cd; padding:2px 6px; border-radius:4px;">Browse File</span>
                button to upload your survey or student data.
            </span>
        </div>
        <div style="margin-bottom: 10px;">
            <span style="font-size:1.25rem; font-weight:700; color:#003366;">Step-2:</span>
            <span style="font-weight:600;"> 
                Explore the Key metrics</span>
                <br>
                <span>Click on the </span><span style="color:#003366; font-weight:bold; background:#e6b0aa; padding:2px 6px; border-radius:4px;"> Go to Key Metrics </span>
                <span>to instantly view key trends and  metrics in your data.
            </span>
        </div>
        <div style="margin-bottom: 10px;">
            <span style="font-size:1.25rem; font-weight:700; color:#003366;">Step-3:</span>
            <span style="font-weight:600;">
                Discover Group-Level Insights</span>
                <br>
                <span>Head to the</span> <span style="color:#003366; font-weight:bold; background:#a3d8d3; padding:2px 6px; border-radius:4px;"> Go to Visualisations </span>
                section to see how different student groups (based on gender, grade, religion, etc) experience belonging in your school.
            </span>
            </div>
            <div style="margin-bottom: 10px;">
                <span style="font-size:1.25rem; font-weight:700; color:#003366;">Step-4:</span>
                <span style="font-weight:600;">
                    Explore Data Tables</span>
                <br>
                    <span>Click the <span style="color:#003366; font-weight:bold; background:#fdf8b7; padding:2px 6px; border-radius:4px;">Go to data tables</span>
                    button to get access to data tables in one view. 
                </span>
            </div>
            <div style="margin-bottom: 10px;">
                <span style="font-size:1.25rem; font-weight:700; color:#003366;">Step-5:</span>
                <span style="font-weight:600;">
                    Download a Custom Report</span>
                <br>
                    <span>Click the <span style="color:#003366; font-weight:bold; background:#a3d8d3; padding:2px 6px; border-radius:4px;">Generate Report</span>
                    button to get key insights and charts in a PDF format to share with your team!
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )
        
    # Show sample data info directly under Step-4
    st.markdown("### Sample Data Preview")
    st.write("""To get the most out of this tool, your data should include:
    - **Demographic Columns**: e.g., StudentID, Gender, Grade, Religion, Ethnicity
    - **Socio-Economic Status Indicators**: e.g., What items do you have at home? (Car, Laptop, Apna Ghar, etc.)
    - **Survey Responses**: e.g., 'Strongly Agree', 'Agree', 'Neutral', 'Disagree', 'Strongly Disagree'
    """)
    st.write("This tool is designed with care to support schools in building more inclusive, welcoming, and responsive learning environments. We’re excited you’re here!")
    
    show_sample_onboard = st.toggle("Show Sample Data", value=False, key="toggle_sample_onboard")
    if show_sample_onboard:
        st.markdown("### Expected Data Structure")
        st.dataframe(sample_data.head())
    st.download_button(
        label="📥 Download Sample Data",
        data=sample_data.to_csv(index=False),
        file_name="sample_data.csv",
        mime="text/csv"
    )

    # Buttons to navigate
    col1, col2 = st.columns([1, 1])
    with col2:
        if st.button("Start Exploring", key="start_exploring_button", use_container_width=True):
            navigate_to('main')
            st.rerun()
    with col1:
        if st.button("Back to Login", key="back_to_login_from_landing", use_container_width=True):
            # Clear user-specific session state to effectively log out
            if 'logged_in_user' in st.session_state:
                del st.session_state['logged_in_user']
            if 'school_name' in st.session_state:
                del st.session_state['school_name']
            if 'school_logo_base64' in st.session_state:
                del st.session_state['school_logo_base64']
            if 'df_cleaned' in st.session_state:
                del st.session_state['df_cleaned']
            navigate_to('login')
            st.rerun()
    st.stop()

# Questionnaire mapping
questionnaire_mapping = {
    "Strongly Disagree": 1,
    "Disagree": 2,
    "Neutral": 3,
    "Agree": 4,
    "Strongly Agree": 5
}
    
# Main Page
if st.session_state['current_page'] == 'main':
    # Header with Project Apnapan logo and school details on the same line
    col1, col2 = st.columns([4, 4])  # Adjust column widths for alignment

    with col1:
        # Project Apnapan logo and name
        st.markdown(f"""
            <div class="custom-logo">
                <img src="data:image/png;base64,{logo_base64}" alt="Project Apnapan Logo" />
                <span>Project Apnapan</span>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        # School logo and name
        if 'logged_in_user' in st.session_state:
            school_name = st.session_state.get('school_name')
            school_logo_base64 = st.session_state.get('school_logo_base64')

            school_logo_html = ""
            if school_logo_base64:
                school_logo_html = f'<img src="data:image/png;base64,{school_logo_base64}" alt="School Logo" style="height: 50px;" />'

            school_name_html = ""
            if school_name:
                school_name_html = f'<h4 style="margin: 0; color: #003366 !important;">{school_name}</h4>'

            if school_logo_html or school_name_html:
                st.markdown(f"""
                    <div style="display: flex; justify-content: flex-end; align-items: center; gap: 12px; padding-top: 10px;">
                        {school_logo_html}
                        {school_name_html}
                    </div>
                """, unsafe_allow_html=True)

    # Main content starts here
    col1, col2 = st.columns([3, 1])  # Adjust layout for title and other content

    with col1:
        st.title("Data Insights Generator")
        st.write("Explore your data and generate insights.")

    df = None  # Initialize df
    file_source = None  # Track if from upload or history

    # File Uploader with History (MongoDB-based)
    if 'logged_in_user' in st.session_state:
        school_id = st.session_state['logged_in_user']
        
        # List history
        history_files = list_user_files(school_id)
        if history_files:
            st.subheader("Upload New File or Select from History")

            # Create display options with timestamps
            history_options = []
            for f in history_files:
                ts = f.get('timestamp')
                # Ensure timestamp is a datetime object before formatting
                if isinstance(ts, datetime):
                    display_text = f"{f['filename']} (Uploaded: {ts.strftime('%Y-%m-%d %H:%M')})"
                else:
                    display_text = f['filename']  # Fallback if no timestamp
                history_options.append(display_text)

            selected_option = st.selectbox(
                "Select a previous file",
                options=["-- New Upload --"] + history_options,
                help="Choose a previously uploaded file or upload a new one."
            )
            
            if selected_option != "-- New Upload --":
                # Extract the original filename from the selected option
                match = re.match(r"^(.*?) \(Uploaded: .*\)$", selected_option)
                if match:
                    selected_file_name = match.group(1)
                else:
                    selected_file_name = selected_option # Fallback for old files without timestamp
                # Load from history
                downloaded_file = download_file_from_mongo(school_id, selected_file_name)
                if downloaded_file:
                    file_source = "history"
                    st.success(f"Loaded {selected_file_name} from history.")

                    # Read the file content into bytes for the download button
                    # and then rewind the stream for pandas to process it later.
                    file_bytes = downloaded_file.read()
                    downloaded_file.seek(0)

                    st.markdown('<div class="history-download-button">', unsafe_allow_html=True)
                    st.download_button(
                        label=f"Download {selected_file_name}",
                        data=file_bytes,
                        file_name=selected_file_name,
                        mime=get_mime_type(selected_file_name)
                    )
                    st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("No previous files found in your history.")
    
    # Standard Uploader (always show, but if history selected, skip upload)
    if file_source != "history":
        uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx", "xls", "txt"])
        if uploaded_file:
            # Upload to MongoDB if logged in
            if 'logged_in_user' in st.session_state:
                success = upload_file_to_mongo(school_id, uploaded_file)
                if success:
                    st.success(f"File uploaded to your history: {uploaded_file.name}")
            file_source = "upload"

    # Process the File (from upload or history)
    if file_source:
        try:
            if file_source == "history":
                content = downloaded_file  # BytesIO from MongoDB
            else:
                uploaded_file.seek(0)
                content = io.BytesIO(uploaded_file.getvalue())
            
            file_type = (selected_file_name if file_source == "history" else uploaded_file.name).split('.')[-1].lower()
            
            if file_type in ["csv", "txt"]:
                df = pd.read_csv(content)
            elif file_type in ["xlsx", "xls"]:
                df = pd.read_excel(content)
            else:
                st.error("Unsupported file format.")
                st.stop()

            # Your existing data processing (timestamp removal, preview, etc.)
            timestamp_keywords = ['timestamp', 'date', 'time', 'created', 'submitted', 'record', 'entry', 'logged']
            timestamp_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in timestamp_keywords)]
            # if timestamp_cols:
            #     df = df.drop(columns=timestamp_cols)
            #     st.write(f"Removed timestamp columns: {', '.join(timestamp_cols)}")

            # st.write(f"File loaded: {selected_file_name if file_source == 'history' else uploaded_file.name}")
            # st.write(f"File size: {content.getbuffer().nbytes} bytes")
            # st.write(f"Number of columns: {df.shape[1]}")

            st.write("### Data Preview")
            col1, col2 = st.columns([8, 2])
            with col1:
                show_preview = st.toggle("Show Table", value=True, key="toggle_preview")
            if show_preview and "df" in locals():
                st.dataframe(df.head())
                st.session_state["preview_table"] = df.head()

            # --- Centralized Processing: Process Once, Use Many ---
            # This is the core performance improvement. All calculations happen here, once.
            # The results are stored in the session state for other pages to use instantly.
            with st.spinner("Analyzing your data... This may take a moment."):
                # Clear any previous results to ensure a fresh start
                keys_to_clear = [
                    'df_cleaned', 'matched_questions', 'belonging_questions',
                    'overall_belonging_score', 'category_averages', 'highest_area',
                    'lowest_area', 'matched_questions_table', 'summary_table',
                    'category_averages_table'
                ]
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]

                # Process data and calculate all metrics
                processing_results = process_data_and_calculate_metrics(df)
                # Store all results in the session state
                for key, value in processing_results.items():
                    st.session_state[key] = value
            
            st.success("Data analysis complete! You can now explore the metrics and visualizations.")

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            st.stop()

    # Ensure df is not None before accessing its columns
    if df is None:
        st.error("No data available. Please upload a valid file.")
        st.stop()
    # Detect questionnaire columns dynamically

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button(" Back to Landing page", use_container_width=True):
            navigate_to('landing')
            st.rerun()
    with col2:
        if st.button(" Go to Key Metrics", use_container_width=True):
            navigate_to('metrics')
            st.rerun()
    st.stop() 

if st.session_state['current_page'] == 'metrics':
        # Header with Project Apnapan logo and school details on the same line
        col1, col2 = st.columns([4, 4])  # Adjust column widths for alignment

        with col1:
            # Project Apnapan logo and name
            st.markdown(f"""
                <div class="custom-logo">
                    <img src="data:image/png;base64,{logo_base64}" alt="Project Apnapan Logo" />
                    <span>Project Apnapan</span>
                </div>
            """, unsafe_allow_html=True)

        with col2:
            # School logo and name
            if 'logged_in_user' in st.session_state:
                school_name = st.session_state.get('school_name')
                school_logo_base64 = st.session_state.get('school_logo_base64')

                school_logo_html = ""
                if school_logo_base64:
                    school_logo_html = f'<img src="data:image/png;base64,{school_logo_base64}" alt="School Logo" style="height: 50px;" />'

                school_name_html = ""
                if school_name:
                    school_name_html = f'<h4 style="margin: 0; color: #003366 !important;">{school_name}</h4>'

                if school_logo_html or school_name_html:
                    st.markdown(f"""
                        <div style="display: flex; justify-content: flex-end; align-items: center; gap: 12px; padding-top: 10px;">
                            {school_logo_html}
                            {school_name_html}
                        </div>
                    """, unsafe_allow_html=True)
        st.header("Key Metrics (Scale of 5)")

        # --- Retrieve pre-calculated results from session state ---
        # All calculations are now done on the main page for performance.
        # This page just displays the results.
        overall_belonging_score = st.session_state.get("overall_belonging_score")
        category_averages = st.session_state.get("category_averages", {})
        highest_area = st.session_state.get("highest_area")
        lowest_area = st.session_state.get("lowest_area")
        matched_questions_df = st.session_state.get("matched_questions_table")

        # --- Check if data is available ---
        if overall_belonging_score is None:
            st.warning("No data has been processed yet. Please go to the main page and upload a file.")
            if st.button("Back to Upload Page"):
                st.stop()

        # Show Likert scale image above the three score cards
        if scale_base64:
            st.markdown(
            f'''
            <div style="display: flex; justify-content: center; align-items: center;">
                <img src="data:image/png;base64,{scale_base64}" alt="Likert Scale" style="width:70%; max-width:600px; min-width:300px; height:150px; margin-bottom:18px;"/>
            </div>
            ''',
            unsafe_allow_html=True
        )
        # Three-column horizontal layout
        col1, col2, col3 = st.columns(3)

        if overall_belonging_score is not None and category_averages:
            with col1:
                st.markdown(f"""
                    <div style="background-color:#e6b0aa; border: 4px solid #ff9999; border-radius:10px; padding:1rem; text-align:center;
                                box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; height: 120px; display: flex; flex-direction: column; justify-content: center;">
                        <h4> &#9734; Overall Belonging Score</h4>
                        <h4 style="margin:0;">{overall_belonging_score:.2f}</h4>
                    </div>
                """, unsafe_allow_html=True)

            with col2:
                st.markdown(f"""
                    <div style="background-color:#99ccff; border: 4px solid #A7C7E7; border-radius:10px; padding:0.5rem; text-align:center;
                                box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; height: 120px; display: flex; flex-direction: column; justify-content: center;">
                        <h4 style="font-size: 1.5rem; margin: 0;"> Highest Score: {highest_area} </h4>
                        <h2 style="font-size: 1.5rem; margin: 0;">{category_averages[highest_area]:.2f}</h2>
                    </div>
                """, unsafe_allow_html=True)

            with col3:
                if lowest_area is not None:
                    st.markdown(f"""
                        <div style="background-color:#FAC898; border: 4px solid #ffcc00; border-radius:10px; padding:0.5rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; height: 120px; display: flex; flex-direction: column; justify-content: center;">
                            <h4 style="font-size: 1.5rem; margin: 0;">Lowest Score: {lowest_area}</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;">{category_averages[lowest_area]:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                        <div style="background-color:#FAC898; border: 4px solid #ffcc00; border-radius:10px; padding:0.5rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; height: 120px; display: flex; flex-direction: column; justify-content: center;">
                            <h4 style="font-size: 1.5rem; margin: 0;">Lowest Score</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;">N/A</h2>
                        </div>
                    """, unsafe_allow_html=True)
        st.markdown("<hr style='border: 1px dashed black; border-radius: 5px;'>", unsafe_allow_html=True)
        st.subheader("Category-wise Averages")
        # Two-column layout
        left_col, right_col = st.columns([1, 1])
        

        # Right column: Safety, Respect, and Welcome
        with left_col:
            if category_averages:
                if "Safety" in category_averages:
                    st.markdown(f"""
                        <div style="background-color:#DFC5FE; border-radius:10px; padding:1rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; width: 100%; height: 120px; display: flex; flex-direction: column; justify-content: center;">
                            <h4 style="font-size: 1rem; margin: 0;">Safety</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;"">{category_averages['Safety']:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)
                if "Respect" in category_averages:
                    st.markdown(f"""
                        <div style="background-color:#fdf8b7; border-radius:10px; padding:0.5rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; width: 100%; height: 120px; display: flex; flex-direction: column; justify-content: center; margin-top: 1rem;">
                            <h4 style="font-size: 1rem; margin: 0;">Respect</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;">{category_averages['Respect']:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)
                if "Welcome" in category_averages:
                    st.markdown(f"""
                        <div style="background-color:#a3d8d3; border-radius:10px; padding:0.5rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; width: 100%; height: 120px; display: flex; flex-direction: column; justify-content: center; margin-top: 1rem;">
                            <h4 style="font-size: 1rem; margin: 0;">Welcome</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;">{category_averages['Welcome']:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)
        with right_col:
            if category_averages:
                if "Participation" in category_averages:
                    st.markdown(f"""
                        <div style="background-color:#DFC5FE; border-radius:10px; padding:1rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; width: 100%; height: 120px; display: flex; flex-direction: column; justify-content: center;">
                            <h4 style="font-size: 1rem; margin: 0;">Participation</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;"">{category_averages['Participation']:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)
                if "Acknowledgement" in category_averages:
                    st.markdown(f"""
                        <div style="background-color:#fdf8b7; border-radius:10px; padding:0.5rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; width: 100%; height: 120px; display: flex; flex-direction: column; justify-content: center; margin-top: 1rem;">
                            <h4 style="font-size: 1rem; margin: 0;">Acknowledgement</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;">{category_averages['Acknowledgement']:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)
                if "Relationships with Teachers" in category_averages:
                    st.markdown(f"""
                        <div style="background-color:#a3d8d3; border-radius:10px; padding:0.5rem; text-align:center;
                                    box-shadow: 0 2px 5px rgba(0,0,0,0.1); color:black; width: 100%; height: 120px; display: flex; flex-direction: column; justify-content: center; margin-top: 1rem;">
                            <h4 style="font-size: 1rem; margin: 0;">Relationships with Teachers</h4>
                            <h2 style="font-size: 1.5rem; margin: 0;">{category_averages['Relationships with Teachers']:.2f}</h2>
                        </div>
                    """, unsafe_allow_html=True)

        
        st.markdown("<hr style='border: 1px dashed black; border-radius: 5px;'>", unsafe_allow_html=True)

        # if category_averages:
        #     col1, col2 = st.columns([8, 2])
        #     with col1:
        #         show_averages = st.toggle("Show Table", value=False, key="toggle_averages")
        #     if show_averages:
        #         st.dataframe(pd.DataFrame.from_dict(category_averages, orient="index", columns=["Average Score"]).round(2))

        # if not df_cleaned.empty:
        #     summary = df_cleaned.describe()
        #     col1, col2 = st.columns([8, 2])
        #     with col1:
        #         show_summary = st.toggle("Show Summary Table", value=False, key="toggle_summary")
        #     if show_summary:
        #         st.dataframe(summary)
        col1, col2 = st.columns([1, 1])
        with col1:
         if st.button("Back to Upload Page", use_container_width=True):
            navigate_to('main')
            st.rerun()
        with col2:
         if st.button("Go to Visualisations" , use_container_width=True):
            navigate_to('visualisations')
            st.rerun()
        st.stop()            

        # Explore and Customize
if st.session_state['current_page'] == 'visualisations':
    # Header with Project Apnapan logo and school details on the same line
        col1, col2 = st.columns([4, 4])  # Adjust column widths for alignment

        with col1:
            # Project Apnapan logo and name
            st.markdown(f"""
                <div class="custom-logo">
                    <img src="data:image/png;base64,{logo_base64}" alt="Project Apnapan Logo" />
                    <span>Project Apnapan</span>
                </div>
            """, unsafe_allow_html=True)

        with col2:
            # School logo and name
            if 'logged_in_user' in st.session_state:
                school_name = st.session_state.get('school_name')
                school_logo_base64 = st.session_state.get('school_logo_base64')

                school_logo_html = ""
                if school_logo_base64:
                    school_logo_html = f'<img src="data:image/png;base64,{school_logo_base64}" alt="School Logo" style="height: 50px;" />'

                school_name_html = ""
                if school_name:
                    school_name_html = f'<h4 style="margin: 0; color: #003366 !important;">{school_name}</h4>'

                if school_logo_html or school_name_html:
                    st.markdown(f"""
                        <div style="display: flex; justify-content: flex-end; align-items: center; gap: 12px; padding-top: 10px;">
                            {school_logo_html}
                            {school_name_html}
                        </div>
                    """, unsafe_allow_html=True)
                    
        st.header("Visualization Tab")
        # --- Retrieve previously saved values into the same variable names ---
        df_cleaned = st.session_state.get("df_cleaned", None)
        matched_questions = st.session_state.get("matched_questions", {})
        
        belonging_questions = st.session_state.get("belonging_questions", {})
        overall_belonging_score = st.session_state.get("overall_belonging_score", None)
        category_averages = st.session_state.get("category_averages", {})
        highest_area = st.session_state.get("highest_area", None)
        lowest_area = st.session_state.get("lowest_area", None)


        group_columns = {
            "Gender": ["gender", "What gender do you use"],
            "Grade": ["grade", "Which grade are you in"],
            "Income Status": ["Income Category"],
            "Health Condition": ["disability", "health condition"],
            "Ethnicity": ["ethnicity_cleaned"],
            "Religion": ["religion"]
        }

        show_explore = st.toggle("Show Charts", value=True, key="toggle_explore")
        if show_explore and not df_cleaned.empty:
            def categorize_income(possessions: str) -> str:
                if pd.isna(possessions):
                    return "Unknown"
                items = possessions.lower()
                has_car = "car" in items
                has_computer = "computer" in items or "laptop" in items
                has_home = "apna ghar" in items
                is_rented = "rent" in items
                if has_car and has_home:
                    return "High"
                if has_computer or (has_home and not has_car):
                    return "Mid"
                return "Low"

            possessions_col = next((col for col in df_cleaned.columns if "what items among these do you have at home".lower() in col.lower()), None)
            if possessions_col:
                df_cleaned["Income Category"] = df_cleaned[possessions_col].apply(categorize_income)

            st.subheader(" Demographic Overview")
            demographic_cols = {
                "Gender": ["gender", "What gender do you use"],
                "Grade": ["grade", "Which grade are you in"],
                "Religion": ["religion"],
                "Ethnicity": ["ethnicity_cleaned"]
            }

            demographic_data = {}
            for label, keywords in demographic_cols.items():
                matched_col = next((col for col in df_cleaned.columns if any(k.lower() in col.lower() for k in keywords)), None)
                if matched_col:
                    demographic_data[label] = matched_col

            if demographic_data:
                items = list(demographic_data.items())

                for row_i in range(0, len(items), 2):
                    row = st.columns(2)

                    for col_i in range(2):
                        idx = row_i + col_i
                        if idx >= len(items):
                            break

                        label, col_name = items[idx]
                        col = row[col_i]

                        value_counts = df_cleaned[col_name].value_counts(dropna=False).rename_axis(label).reset_index(name='Count')
                        fig = px.pie(
                            value_counts,
                            names=label,
                            values='Count',
                            title=f"{label} Distribution",
                            hole=0.3
                        )

                        num_categories = len(value_counts)
                        if num_categories > 3 or any(len(str(cat)) > 8 for cat in value_counts[label]):
                            fig.update_traces(
                                textposition='auto',
                                textinfo='percent',
                                textfont=dict(size=15),
                                marker=dict(line=dict(color='#000000', width=1))
                            )
                        else:
                            fig.update_traces(
                                textposition='auto',
                                textinfo='percent',
                                textfont=dict(size=15)
                            )

                        fig.update_layout(
                            uniformtext_minsize=7,
                            margin=dict(t=45, b=45, l=45, r=45),
                            height=400,
                            width=400,
                            showlegend=True
                        )

                        config = {
                            'displayModeBar': True,
                            'modeBarButtonsToAdd': ['zoom2d', 'autoScale2d', 'resetScale2d', 'toImage'],
                            'toImageButtonOptions': {
                                'format': 'png',
                                'filename': f'{label}_distribution',
                                'height': 500,
                                'width': 700
                            }
                        }

                        col.plotly_chart(fig, use_container_width=True, config=config)


            st.write("### Food for Thought")
            st.write(
                """
                Take a moment to observe the differences in the following charts.  
                - Do certain groups consistently score higher or lower? Why do you think that happens? 
                - What kind of experiences or challenges could be influencing their responses?  
                - Are there social, cultural, or school-related factors that might be shaping these patterns?

                """
            ) 
            st.write("")



            selected_area = st.selectbox("Which belonging aspect do you want to explore?", list(belonging_questions.keys()))
            if selected_area and not df_cleaned.empty:
                area_keywords = belonging_questions[selected_area]
                matched_cols = [col for col in df_cleaned.columns if any(k.lower() in col.lower() for k in area_keywords)]
                if not matched_cols:
                    st.warning("No matching questions found for this aspect.")
                else:
                    target_col= matched_cols[0]
                    st.markdown(f"**Showing results for:** {', '.join(matched_cols)}")
                    
                    


                    col1, col2 = st.columns(2)
                    col_slots = [col1, col2]
                    chart_index = 0

                    group_columns = {
                        "Gender": ["gender", "What gender do you use"],
                        "Grade": ["grade", "Which grade are you in"],
                        "Income Status": ["Income Category"],
                        "Health Condition": ["disability", "health condition"],
                        "Ethnicity": ["ethnicity_cleaned"],
                        "Religion": ["religion"]
                    }
                    
                    # Gave a white box that looked unclean in most charts 
                    # st.markdown(   
                    #     """
                    #     <style>
                    #     .modebar {
                    #         display: block !important;
                    #         background-color: white !important;
                    #         border: 1px solid #ddd !important;
                    #         border-radius: 4px !important;
                    #         padding: 2px !important;
                    #     }
                    #     .modebar-group {
                    #         display: flex !important;
                    #         align-items: center !important;
                    #     }
                    #     .modebar-btn {
                    #         background-color: transparent !important;
                    #         border: none !important;
                    #         padding: 2px 6px !important;
                    #         color: #333333 !important;
                    #     }
                    #     .modebar-btn:hover {
                    #         background-color: #f0f0f0 !important;
                    #     }
                    #     </style>
                    #     """,
                    #     unsafe_allow_html=True
                    # )

                    st.markdown(
                        """
                        <style>
                        .modebar {
                            background-color: transparent !important;
                            border: none !important;
                            box-shadow: none !important;
                        }
                        .modebar-btn > svg { 
                            stroke: white !important;
                            fill: white !important;
                            opacity: 1 !important 
                        }
                        .modebar-btn:hover {
                            background-color: rgba(255, 255, 255, 0.2) !important;
                        }
                        </style>
                        """,
                        unsafe_allow_html=True
                    )


                    for label, keywords in group_columns.items():
                        matched_group_col = next((col for col in df_cleaned.columns if any(k.lower() in col.lower() for k in keywords)), None)
                        if matched_group_col:
                            if "ethnicity" in matched_group_col.lower() and "ethnicity_cleaned" in df_cleaned.columns:
                                plot_df = df_cleaned[["ethnicity_cleaned", target_col]].dropna()
                                plot_df.rename(columns={"ethnicity_cleaned": matched_group_col}, inplace=True)
                            else:
                                plot_df = df_cleaned[[matched_group_col, target_col]].dropna()
                            if target_col in plot_df.columns:
                                plot_df[target_col] = pd.to_numeric(plot_df[target_col], errors="coerce")
                            else:
                                st.warning(f"Column '{target_col}' not found in the data.")
                            group_avg = plot_df.groupby(matched_group_col)[target_col].agg(['mean', 'count']).reset_index()
                            group_avg.columns = [matched_group_col, 'AvgScore', 'Count']

                            # Special handling for 'Grade' to ensure correct numeric sorting.
                            if label == "Grade":
                                # Convert grade to a numeric type for sorting, coercing errors for non-numeric grades
                                group_avg[matched_group_col] = pd.to_numeric(group_avg[matched_group_col], errors='coerce')
                                group_avg = group_avg.sort_values(by=matched_group_col).dropna(subset=[matched_group_col])

                            with col_slots[chart_index % 2]:
                                fig = px.bar(
                                    group_avg,
                                    x=matched_group_col,
                                    y="AvgScore",
                                    text="Count",
                                    title=f"{selected_area} by {label}",
                                    labels={matched_group_col: label, "AvgScore": "Avg Score"},
                                    height=400,
                                    color=matched_group_col,
                                    color_discrete_sequence=px.colors.qualitative.Set3
                                )
                                fig.update_traces(
                                    texttemplate='N=%{text}',
                                    textposition='inside',
                                    width=0.5,
                                    insidetextanchor='middle',
                                    hovertemplate="%{x}<br>Avg Score: %{y:.2f}<br>Students: %{text}<extra></extra>"
                                )
                                for i, row in group_avg.iterrows():
                                    fig.add_annotation(
                                        x=row[matched_group_col],
                                        y=row["AvgScore"],
                                        text=f"Avg={row['AvgScore']:.2f}",
                                        showarrow=False,
                                        yshift=10,
                                        font=dict(color='white'),
                                        bgcolor='rgba(0,0,0,0.5)'
                                    )
                                max_y = group_avg["AvgScore"].max()
                                fig.update_layout(
                                    margin=dict(t=50),
                                    yaxis=dict(range=[0, max_y + 0.5]),
                                )
                                # For the Grade chart, explicitly set the x-axis to 'category'.
                                # This is the most reliable way to prevent Plotly from creating a
                                # continuous axis with decimal points for numeric-like labels.
                                if label == "Grade":
                                    fig.update_xaxes(type='category')
                                config = {
                                    'displayModeBar': True,
                                    'modeBarButtonsToRemove': [
                                    'pan2d', 'select2d', 'lasso2d', 'zoom2d', 'autoScale2d', 'hoverClosestCartesian',
                                    'hoverCompareCartesian', 'toggleSpikelines', 'zoomInGeo', 'zoomOutGeo',
                                    'resetGeo', 'hoverClosestGeo', 'sendDataToCloud', 'toggleHover', 'drawline',
                                    'drawopenpath', 'drawclosedpath', 'drawcircle', 'drawrect', 'eraseshape'
                                    ],
                                    'modeBarButtonsToAdd': ['zoomIn2d', 'zoomOut2d', 'resetScale2d', 'toImage', 'toggleFullscreen'],
                                    #'modeBarButtonsToAdd': ['zoom2d', 'autoScale2d', 'resetScale2d', 'toImage'],
                                    'toImageButtonOptions': {
                                        'format': 'png',
                                        'filename': 'Bar_chart_screenshot',
                                        'height': 500,
                                        'width': 700
                                    },
                                    'displaylogo': False
                                }
                                st.plotly_chart(fig, use_container_width=True, config=config)
                            chart_index += 1
                        else:
                            st.info(f"No data found for {label}.")

                # 🎯 Breakdown by Group (Percentage)
            st.markdown("### Breakdown by Group (Percentage)")
            show_breakdown = st.toggle("Show Chart", value=True, key="toggle_breakdown")
            if show_breakdown:
                breakdown_col = next((col for col in df_cleaned.columns if any(k.lower() in col.lower() for k in group_columns["Gender"])), None)
                if breakdown_col and target_col:
                    breakdown_df = df_cleaned[[breakdown_col, target_col]].dropna()
                    breakdown_df[target_col] = pd.to_numeric(breakdown_df[target_col], errors="coerce")
                    if not breakdown_df.empty:
                        def label_bucket(val):
                            if pd.isna(val):
                                return "Unknown"
                            if val <= 2:
                                return "Disagree"
                            elif val == 3:
                                return "Neutral"
                            elif val >= 4:
                                return "Agree"
                            return "Unknown"
                        breakdown_df["ResponseLevel"] = breakdown_df[target_col].apply(label_bucket)
                        percent_df = breakdown_df.groupby([breakdown_col, "ResponseLevel"]).size().reset_index(name='Count')
                        total_counts = percent_df.groupby(breakdown_col)['Count'].transform('sum')
                        percent_df['Percent'] = (percent_df['Count'] / total_counts * 100).round(1)
                        response_order = ["Agree", "Neutral", "Disagree", "Unknown"]
                        percent_df["ResponseLevel"] = pd.Categorical(percent_df["ResponseLevel"], categories=response_order, ordered=True)
                        percent_df["text"] = percent_df.apply(lambda row: f"{row['Percent']}% ({row['Count']} students)", axis=1
)
                        fig = px.bar(
                            percent_df,
                            x=breakdown_col,

                            color="ResponseLevel",
                            y=percent_df["Percent"].astype(str) + '%',                            text="text",
                            barmode="stack",
                            title=f"Percentage Breakdown of Responses to '{selected_area}' by Gender",
                            color_discrete_map={
                                "Agree": "#4CAF50",
                                "Neutral": "#FFC107",
                                "Disagree": "#F44336",
                                "Unknown": "#9E9E9E"
                            },
                            height=450
                        )
                        fig.update_layout(
                            yaxis_title="Percentage (%)",
                            xaxis_title=breakdown_col,
                            bargap=0.2,
                            legend_title="Response Level",
                            uniformtext_minsize=8,
                            uniformtext_mode='hide'
                        )
                        fig.update_traces(
                            textposition="inside",
                            insidetextanchor="middle",
                            cliponaxis=False
                        )
                        config = {
                            'displayModeBar': True,
                            'modeBarButtonsToRemove': [
                                'pan2d', 'select2d', 'lasso2d', 'zoom2d', 'autoScale2d', 'hoverClosestCartesian',
                                'hoverCompareCartesian', 'toggleSpikelines', 'zoomInGeo', 'zoomOutGeo',
                                'resetGeo', 'hoverClosestGeo', 'sendDataToCloud', 'toggleHover', 'drawline',
                                'drawopenpath', 'drawclosedpath', 'drawcircle', 'drawrect', 'eraseshape'
                            ],
                            'modeBarButtonsToAdd': ['zoomIn2d', 'zoomOut2d', 'resetScale2d', 'toImage', 'toggleFullscreen'],
                            'toImageButtonOptions': {
                                'format': 'png',
                                'filename': 'bar_chart_screenshot',
                                'height': 500,
                                'width': 700,
                                'scale': 2
                            },
                                        'displaylogo': False
                        }


                        st.plotly_chart(fig, use_container_width=True, config=config)
        
        col1, col2 = st.columns([1, 1])

        with col1:
            if st.button("Back to Key Metrics", use_container_width=True):
                navigate_to('metrics')
                st.rerun()

        with col2:
            if st.button("Go to Data Tables", use_container_width=True):
                navigate_to('data_table')
                st.rerun()
        st.stop()




if st.session_state['current_page'] == 'data_table':
    # Header with Project Apnapan logo and school details on the same line
    col1, col2 = st.columns([4, 4])  # Adjust column widths for alignment

    with col1:
        # Project Apnapan logo and name
        st.markdown(f"""
            <div class="custom-logo">
                <img src="data:image/png;base64,{logo_base64}" alt="Project Apnapan Logo" />
                <span>Project Apnapan</span>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        # School logo and name
        if 'logged_in_user' in st.session_state:
            school_name = st.session_state.get('school_name')
            school_logo_base64 = st.session_state.get('school_logo_base64')

            school_logo_html = ""
            if school_logo_base64:
                school_logo_html = f'<img src="data:image/png;base64,{school_logo_base64}" alt="School Logo" style="height: 50px;" />'

            school_name_html = ""
            if school_name:
                school_name_html = f'<h4 style="margin: 0; color: #003366 !important;">{school_name}</h4>'

            if school_logo_html or school_name_html:
                st.markdown(f"""
                    <div style="display: flex; justify-content: flex-end; align-items: center; gap: 12px; padding-top: 10px;">
                        {school_logo_html}
                        {school_name_html}
                    </div>
                """, unsafe_allow_html=True)
    st.header(" Data Tables")

    # Initialize state for PDF generation to prevent re-generation on every interaction
    if 'pdf_buffer' not in st.session_state:
        st.session_state.pdf_buffer = None

    # ---- pull from session_state (no hardcoded numbers) ----
    df_cleaned          = st.session_state.get("df_cleaned", None)
    matched_questions   = st.session_state.get("matched_questions", {})
    category_averages   = st.session_state.get("category_averages", {})
    overall_belonging   = st.session_state.get("overall_belonging_score", None)
    highest_area        = st.session_state.get("highest_area", None)
    lowest_area         = st.session_state.get("lowest_area", None)

    # ---- Fetch school details for the report ----
    school_name = "Your School" # Default
    school_logo_base64 = None
    if 'logged_in_user' in st.session_state:
        name = st.session_state.get('school_name')
        logo = st.session_state.get('school_logo_base64')
        if name:
            school_name = name
        if logo:
            school_logo_base64 = logo

    date_today  = date.today().strftime("%d %B, %Y")
    n_students  = int(df_cleaned.shape[0]) if isinstance(df_cleaned, pd.DataFrame) else 0

    # ---- Tables (as you had) ----
    st.write("### Data Preview")
    if "preview_table" in st.session_state:
        st.dataframe(st.session_state["preview_table"])
    else:
        st.info("No preview table saved yet.")

    st.write("### Matched Questions")
    if "matched_questions_table" in st.session_state:
        st.dataframe(st.session_state["matched_questions_table"])
    else:
        # build once if not saved
        if isinstance(matched_questions, dict) and matched_questions:
            mq_df = pd.DataFrame.from_dict(matched_questions, orient="index").T.fillna("")
            st.session_state["matched_questions_table"] = mq_df
            st.dataframe(mq_df)
        else:
            st.info("No matched questions available.")

    st.write("### Category Averages")
    if category_averages:
        averages_df = pd.DataFrame.from_dict(category_averages, orient="index", columns=["Average Score"]).round(2)
        st.dataframe(averages_df)
        st.session_state["category_averages_table"] = averages_df
    else:
        st.info("No category averages available.")

    if isinstance(df_cleaned, pd.DataFrame) and not df_cleaned.empty:
        summary = df_cleaned.describe()
        st.write("### Summary Table ")
        st.dataframe(summary)
        st.session_state["summary_table"] = summary
    else:
        st.info("No cleaned data available.")

    # ========= PDF GENERATION =========
    # helpers to draw pies with matplotlib and return BytesIO for ReportLab
    def pie_image_from_series(series, title):
        """
        Generates a more readable pie chart PNG in a BytesIO buffer.
        It avoids overlapping labels by using a legend for numerous categories.
        series: pandas.Series of counts (value_counts)
        title: The title for the chart.
        returns: BytesIO PNG or None if data is empty.
        """
        buf = io.BytesIO()
        labels = series.index.astype(str).tolist()
        sizes = series.values.tolist()
        if not sizes:
            return None

        # Use a legend if there are more than 4 categories to prevent label overlap
        show_labels_on_pie = len(labels) <= 4

        # Adjust figure size to accommodate legend if needed
        figsize = (4.5, 3) if not show_labels_on_pie else (3, 3)
        fig, ax = plt.subplots(figsize=figsize, dpi=200)

        # Use the default Plotly color sequence to match the demographic overview charts
        plotly_default_colors = [
            '#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
            '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'
        ]
        # Cycle through the defined colors if there are more labels than colors
        colors_map = [plotly_default_colors[i % len(plotly_default_colors)] for i in range(len(labels))]

        wedges, texts, autotexts = ax.pie(
            sizes,
            autopct=lambda p: f'{p:.1f}%' if p > 1 else '',  # Only show percentage for slices > 1%
            startangle=90,
            colors=colors_map,
            pctdistance=0.8,  # Move percentage inside the slice
            labels=labels if show_labels_on_pie else None,
            labeldistance=1.1,
            textprops={'fontsize': 7}  # Smaller font for labels on pie
        )

        # Style the percentage text for better visibility
        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_weight('bold')
            autotext.set_fontsize(7)

        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        ax.set_title(title, fontsize=10, pad=15)

        # If not showing labels on pie, add a legend outside the chart
        if not show_labels_on_pie:
            ax.legend(wedges, labels,
                      title="Categories",
                      loc="center left",
                      bbox_to_anchor=(1, 0, 0.5, 1),
                      fontsize='x-small')

        fig.tight_layout()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf

    # Build demographic pies (only if columns exist)
    gender_pie_buf = None
    religion_pie_buf = None
    if isinstance(df_cleaned, pd.DataFrame) and not df_cleaned.empty:
        # Try to find likely columns
        gender_col = next((c for c in df_cleaned.columns if "gender" in c.lower()), None)
        religion_col = next((c for c in df_cleaned.columns if "relig" in c.lower()), None)

        if gender_col:
            gender_counts = df_cleaned[gender_col].astype(str).replace({"nan": "Unknown"}).value_counts(dropna=False)
            gender_pie_buf = pie_image_from_series(gender_counts, "Gender Distribution")

        if religion_col:
            religion_counts = df_cleaned[religion_col].astype(str).replace({"nan": "Unknown"}).value_counts(dropna=False)
            religion_pie_buf = pie_image_from_series(religion_counts, "Religion Distribution")

    # # Build constructs list table (right-rail look)
    constructs_table_data = [["Construct", "Avg (1–5)"]]
    if category_averages:
        for k, v in category_averages.items():
            constructs_table_data.append([k, f"{float(v):.2f}"])
    else:
        constructs_table_data.append(["-", "-"])

    # bubble styles in reportlab via 1x1 tables with background colors
    def bubble(text, bg_hex):
        return Table([[Paragraph(text, ParagraphStyle("bub", fontSize=12, alignment=1, textColor=colors.white))]],
                     colWidths=[2.2*inch], rowHeights=[0.9*inch],
                     style=TableStyle([
                         ("BACKGROUND", (0,0), (-1,-1), colors.HexColor(bg_hex)),
                         ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                         ("ALIGN", (0,0), (-1,-1), "CENTER"),
                         ("BOX", (0,0), (-1,-1), 0, colors.white),
                         ("INNERGRID", (0,0), (-1,-1), 0, colors.white),
                     ]))

    def generate_pdf(school_name, school_logo_base64, apnapan_logo_base64):
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=28, rightMargin=28, topMargin=28, bottomMargin=28)
        styles = getSampleStyleSheet()

        # Custom styles
        title_style   = ParagraphStyle("TitleStyle", parent=styles["Title"], fontSize=18, alignment=1, textColor=colors.black, spaceAfter=6)
        small_grey    = ParagraphStyle("SmallGrey", parent=styles["Normal"], fontSize=9, alignment=2, textColor=colors.HexColor("#666"))
        header_style  = ParagraphStyle("HeaderStyle", parent=styles["Heading2"], fontSize=14, alignment=1, textColor=colors.HexColor("#000"))
        note_style    = ParagraphStyle("NoteStyle", parent=styles["Normal"], fontSize=10)

        story = []

        # --- PDF Header with Logos and Names ---
        apnapan_logo_img = Paragraph(" ", styles['Normal']) # Fallback
        if apnapan_logo_base64:
            try:
                apnapan_logo_bytes = io.BytesIO(base64.b64decode(apnapan_logo_base64))
                apnapan_logo_img = Image(apnapan_logo_bytes, width=0.8*inch, height=0.8*inch)
            except Exception:
                pass # Keep fallback

        school_logo_img = Paragraph(" ", styles['Normal']) # Fallback
        if school_logo_base64:
            try:
                school_logo_bytes = io.BytesIO(base64.b64decode(school_logo_base64))
                school_logo_img = Image(school_logo_bytes, width=0.8*inch, height=0.8*inch)
            except Exception:
                pass # Keep fallback

        # Center content: Report Title and School Name
        center_content = [
            Paragraph("Apnapan Pulse Report", title_style),
            Paragraph(school_name, header_style)
        ]

        header_table = Table([[apnapan_logo_img, center_content, school_logo_img]], colWidths=[1*inch, 5.5*inch, 1*inch])
        header_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
        ]))
        story.append(header_table)
        story.append(Paragraph(f"Date: {date_today}", small_grey))
        story.append(Spacer(1, 12))

        # Row of two big bubbles: Belonging score + N students
        score_txt = f"<b>Belonging score</b><br/><br/><font size=18>{(overall_belonging or 0):.2f}</font>"
        n_txt     = f"<b>Number of students (N)</b><br/><br/><font size=18>{n_students}</font>"

        row1 = Table([[bubble(score_txt, "#E59A86"), bubble(n_txt, "#A7E181")]],
                     colWidths=[3.1*inch, 3.1*inch])
        story.append(row1)
        story.append(Spacer(1, 10))

        # Strongest / Weakest bubbles
        strong_label = (highest_area if isinstance(highest_area, str) else "-")
        strong_val   = float(category_averages.get(strong_label, 0)) if strong_label in category_averages else 0.0
        weak_label   = (lowest_area if isinstance(lowest_area, str) else "-")
        weak_val     = float(category_averages.get(weak_label, 0)) if weak_label in category_averages else 0.0

        strong_txt = f"<b>Strongest area:</b><br/>{strong_label}<br/><font size=13>{strong_val:.2f}</font>"
        weak_txt   = f"<b>Weakest area:</b><br/>{weak_label}<br/><font size=13>{weak_val:.2f}</font>"
        row2 = Table([[bubble(strong_txt, "#636EFA"), bubble(weak_txt, "#FFD29B")]],
                     colWidths=[3.1*inch, 3.1*inch])
        story.append(row2)
        story.append(Spacer(1, 14))

        # Section: Demographics + Right rail constructs
        left_cells = []
        # Add pies if available
        pie_elems = []
        if gender_pie_buf:
            pie_elems.append(Image(gender_pie_buf, width=2.5*inch, height=2.5*inch))
        if religion_pie_buf:
            pie_elems.append(Image(religion_pie_buf, width=2.7*inch, height=2.7*inch))
        if pie_elems:
            left_cells.extend(pie_elems)
        else:
            left_cells.append(Paragraph("Demographics charts will appear here when available.", note_style))

        left_flow = []
        for elem in left_cells:
            left_flow.append(elem)
            left_flow.append(Spacer(1, 10))

        constructs_tbl = Table(constructs_table_data, colWidths=[2.1*inch, 1.0*inch])
        constructs_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
            ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#AAAAAA")),
            ("ALIGN", (1,1), (1,-1), "CENTER"),
        ]))
        right_panel = []
        right_panel.append(Paragraph("Construct Scores", header_style))
        right_panel.append(Spacer(1, 6))
        right_panel.append(constructs_tbl)

        # Two-column layout: left (pies) | right (constructs list)
        layout = Table([[left_flow, right_panel]], colWidths=[3.9*inch, 2.3*inch])
        layout.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))
        story.append(layout)
        story.append(Spacer(1, 16))

        # Food for Thought
        story.append(Paragraph("Food for Thought", header_style))
        bullets = """
        <ul>
            <li>Do certain groups consistently score higher or lower?</li>
            <li>Why do you think that happens?</li>
            <li>What kind of experiences or challenges could be influencing their responses?</li>
            <li>Are there social, cultural, or school-related factors that might be shaping these patterns?</li>
        </ul>
        """
        story.append(Paragraph(bullets, styles['Normal']))

        doc.build(story)
        buffer.seek(0)
        return buffer

    # ---- Top row buttons: Back + Feedback side-by-side ----
    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        if st.button(" Back to Visualisations", use_container_width=True):
            st.session_state.pdf_buffer = None # Clear buffer when navigating away
            navigate_to('visualisations')
            st.rerun()
    with colB:
        # clicking sets a flag that opens the expander automatically below
        # ---- Feedback section ----
        if "show_feedback_form" not in st.session_state:
            st.session_state["show_feedback_form"] = False

        # Feedback button logic
        if st.button("Feedback", use_container_width=True, key="feedback_button_main"):
            st.session_state["show_feedback_form"] = not st.session_state["show_feedback_form"]

        # Feedback form (conditionally displayed)
    if st.session_state["show_feedback_form"]:
        st.write("### Feedback")
        feedback = st.text_area("Flag any issues or suggestions", key="feedback_text_area")
        if st.button("Submit Feedback", key="submit_feedback_button"):
            if feedback:
                try:
                    sheet = connect_to_google_sheet("Apnapan Data Insights Generator Tool Feedbacks")
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    sheet.append_row([timestamp, feedback])
                    st.success("Thank you! Your feedback has been recorded.")
                    st.session_state["show_feedback_form"] = False  # Close the form after submission
                except Exception as e:
                    st.error(f"Failed to send feedback: {e}")
            else:
                st.warning("Please enter some feedback before submitting.")

    # ---- Styled download button (peach) ----
    st.markdown("""
        <style>
        .report-download-button .stDownloadButton > button {
            background-color: #C7361A !important;
            color: white !important;
            border-radius: 10px;
            border: none;
            font-weight: 700;
        }
        .report-download-button .stDownloadButton > button:hover {
            background-color: #FFB774 !important;
        }
        </style>
    """, unsafe_allow_html=True)

    with colC:
        # The "Generate" button is the primary action. It creates the PDF and stores it in state.
        if st.button("Generate Report", use_container_width=True, key="generate_report"):
            with st.spinner("Generating your report..."):
                st.session_state.pdf_buffer = generate_pdf(school_name, school_logo_base64, logo_base64)

        # If a report has been generated, show the download button.
        if st.session_state.get('pdf_buffer'):
            st.markdown('<div class="report-download-button">', unsafe_allow_html=True)
            st.download_button(
                label="Download Report",
                data=st.session_state.pdf_buffer,
                use_container_width=True,
                file_name="Apnapan_Pulse_Report.pdf",
                mime="application/pdf"
            )
            st.markdown('</div>', unsafe_allow_html=True)

    with st.expander("Need Help?"):
        st.write("Contact us at Email: projectapnapan@gmail.com")

    st.stop()