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
database = 'sdl2'
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
            sub2  = re.compile(r",[A-Z][A-Z]\d{5}")
            sub1  = re.compile(r" [A-Z][A-Z]\d{5}")
            practical = re.compile(r"P-[A-Z][A-Z]\d{5}")

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
                        subjects1 = re.findall(sub1,line)
                        subjects2 = re.findall(sub2,line)
                        practicals = re.findall(practical,line)
                        kt_count = line.count(',') +1
                        if practicals:
                            answer = "".join(subjects1) + "".join(subjects2) + "," + ",".join(practicals)
                        else:
                            answer = "".join(subjects1) + "".join(subjects2)
                        insert_statement = f"""INSERT INTO student (roll_no, {sem},kt_count) VALUES (%s, %s, %s)"""
                        variables = (temp.group(), answer,kt_count)
                        cur.execute(insert_statement, variables)
                    if lateral.search(line):
                        temp = lateral.search(line)
                        subjects1 = re.findall(sub1,line)
                        subjects2 = re.findall(sub2,line)
                        kt_count = line.count(',') +1
                        if practicals:
                            answer = "".join(subjects1) + "".join(subjects2) + "," + ",".join(practicals)
                        else:
                            answer = "".join(subjects1) + "".join(subjects2)
                        insert_statement = f"""INSERT INTO student (roll_no, {sem},kt_count) VALUES (%s, %s, %s)"""
                        variables = (temp.group(), answer,kt_count)
                        cur.execute(insert_statement, variables)

        cur.execute("""
            CREATE TABLE final AS (
                SELECT 
                    roll_no,
                    STRING_AGG(ktsem1, '') AS ktsem1,
                    STRING_AGG(ktsem2, '') AS ktsem2,
                    STRING_AGG(ktsem3, '') AS ktsem3,
                    STRING_AGG(ktsem4, '') AS ktsem4,
                    SUM(kt_count) AS kt_count
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
            kts = record['kt_count']
            vari = str(record['roll_no'])
            if kts > 5:
                update_entry = f"""UPDATE final
                                   SET result = 'SEM BACK' 
                                   WHERE roll_no = %s"""
                cur.execute(update_entry, (vari,))
        cur.execute("""
                      CREATE TABLE List AS 
                    (
                          SELECT *, 
                          CONCAT('20', SUBSTRING(roll_no, 7, 2), '-',
                          RIGHT('0' || (SUBSTRING(roll_no, 7, 2)::INTEGER + 1), 2)) AS Scheme
                          FROM final 
                          ORDER BY roll_no
                    )
                 """)
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
st.title("Cross List Processing Application")
st.write("Upload a Cross List in PDF format to process and extract student data.")

uploaded_file = st.file_uploader("Choose a PDF file", type=["pdf"])

if uploaded_file is not None:
    if allowed_file(uploaded_file.name):
        file_path = uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        process_pdf(file_path)
    else:
        st.error("Invalid file type. Please upload a PDF file.")
