import streamlit as st
import os
import re
import pdfplumber
import psycopg2
import psycopg2.extras
import pandas as pd

# Streamlit configurations
st.set_page_config(page_title="PDF Processing App", layout="wide")

# Database connection settings
hostname = 'localhost'
database = 'SDLproject'
username = 'postgres'
pwd = 'Air@8888'
port_id = 5432

# Define allowed file extensions
ALLOWED_EXTENSIONS = {'pdf'}

# Function to check allowed file extensions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Process PDF and insert data into the database
def process_pdf(file_path):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=hostname,
            user=username,
            dbname=database,
            password=pwd,
            port=port_id
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        with pdfplumber.open(file_path) as pdf:
            num_pages = len(pdf.pages)
            goal = re.compile(r"0801[A-Z][A-Z]\d{6}")
            lateral = re.compile(r"0801[A-Z][A-Z]\d{3}[A-Z]\d{2}")
            sem1 = re.compile(r"First Semester")
            sem2 = re.compile(r"Second Semester")
            sem3 = re.compile(r"IIIrd Semester")
            sem4 = re.compile(r"IVth Semester")

            cur.execute("""TRUNCATE student;""")
            cur.execute("""DROP TABLE IF EXISTS final;""")
            cur.execute("""DROP TABLE IF EXISTS List""")

            for i in range(num_pages):
                page = pdf.pages[i]
                text = page.extract_text()
                sem = 'ktsem1'
                if sem2.search(text):
                    sem = 'ktsem2'
                if sem3.search(text):
                    sem = 'ktsem3'
                if sem4.search(text):
                    sem = 'ktsem4'
                for line in text.split('\n'):
                    if goal.search(line):
                        temp = goal.search(line)
                        comma_count = line.count(',')
                        insert_statement = f"""INSERT INTO student (roll_no, {sem}) VALUES (%s, %s)"""
                        variables = (temp.group(), comma_count + 1)
                        cur.execute(insert_statement, variables)
                    if lateral.search(line):
                        temp = lateral.search(line)
                        comma_count = line.count(',')
                        insert_statement = f"""INSERT INTO student (roll_no, {sem}) VALUES (%s, %s)"""
                        variables = (temp.group(), comma_count + 1)
                        cur.execute(insert_statement, variables)

        cur.execute("""
            CREATE TABLE final AS (
                SELECT 
                    roll_no,
                    SUM(ktsem1) AS ktsem1,
                    SUM(ktsem2) AS ktsem2,
                    SUM(ktsem3) AS ktsem3,
                    SUM(ktsem4) AS ktsem4
                FROM 
                    student
                GROUP BY 
                    roll_no
                ORDER BY
                    roll_no
            )
        """)
        cur.execute("""
            ALTER TABLE final
            ADD result VARCHAR(20) DEFAULT 'Can Go To Next Sem'
        """)
        cur.execute("SELECT * FROM final")
        for record in cur.fetchall():
            sum = record['ktsem1'] + record['ktsem2'] + record['ktsem3'] + record['ktsem4']
            vari = str(record['roll_no'])
            if sum > 5:
                update_entry = f"""UPDATE final
                                   SET result = 'SEM BACK' 
                                   WHERE roll_no = %s"""
                cur.execute(update_entry, (vari,))
        cur.execute("""CREATE TABLE List AS (SELECT * FROM final ORDER BY roll_no)""")
        cur.execute("""DROP TABLE final""")
        conn.commit()

        # To download the file
        output_file = "output.csv"
        copy_query = f"COPY List TO STDOUT WITH CSV HEADER"
        with open(output_file, "w") as f:
            cur.copy_expert(copy_query, f)
        cur.close()

        st.success("PDF processed successfully!")
        st.download_button(label="Download CSV", data=open(output_file, 'rb').read(), file_name="output.csv")

    except Exception as error:
        st.error(f"An error occurred: {error}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Streamlit UI
st.title("PDF Processing Application")
st.write("Upload a PDF file to process and extract student data.")

uploaded_file = st.file_uploader("Choose a PDF file", type=["pdf"])

if uploaded_file is not None:
    if allowed_file(uploaded_file.name):
        file_path = uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        process_pdf(file_path)
    else:
        st.error("Invalid file type. Please upload a PDF file.")
