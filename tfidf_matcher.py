import pandas as pd
import json
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Specify the path to your JSON file
json_file_path = os.path.join(os.path.dirname(__file__), 'data', 'database.json') 

# Read the JSON file
try:
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)
except FileNotFoundError:
    print(f"Error: File '{json_file_path}' not found.")
    exit()
except json.JSONDecodeError:
    print(f"Error: File '{json_file_path}' contains invalid JSON.")
    exit()

# Check if the JSON data is a list or a single record
if not isinstance(json_data, list):
    json_data = [json_data]  # Convert single record to list for consistent processing

# Load the Excel file
excel_file_path = os.path.join(os.path.dirname(__file__), 'data', 'uscis.xlsx')  # Replace with your actual path
try:
    excel_data = pd.read_excel(excel_file_path)
except FileNotFoundError:
    print(f"Error: Excel file '{excel_file_path}' not found.")
    exit()

excel_data.dropna(subset=['Employer (Petitioner) Name'], inplace=True)  # Drop rows with missing company names in Excel data


def send_batch_email_notification(matching_jobs, recipient_email):
    """Send a single email with all matching companies."""
    try:
        # Email settings
        sender_email = os.environ.get("SENDER_EMAIL")  # Replace with your Gmail
        print(f"Sender email: {sender_email}")  # Debugging line to check sender email
        sender_password = os.environ.get("SENDER_PASSWORD") 
        print(sender_password)
        print(type(sender_password))
        print(f"Sender password: {'*' * len(sender_password) if sender_password else 'Not Set'}")  # Mask password for security
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Job Match Alert: {len(matching_jobs)} H-1B Sponsoring Companies"
        msg['From'] = sender_email
        msg['To'] = recipient_email
        
        # Create HTML content
        html = f"""
        <html>
          <head></head>
          <body>
            <h2>Job Opportunities at H-1B Sponsoring Companies</h2>
            <p>We found {len(matching_jobs)} jobs at companies known to sponsor H-1B visas:</p>
            <table border="1" cellpadding="5">
              <tr>
                <th>Company</th>
                <th>Matched Company</th>
                <th>Job Title</th>
                <th>Match Score</th>
                <th>Link</th>
              </tr>
        """
        
        # Add each matching job to the email
        for job in matching_jobs:
            html += f"""
              <tr>
                <td>{job['company']}</td>
                <td>{job['matched_company']}</td>
                <td>{job['title']}</td>
                <td>{job['match_score']:.2f}</td>
                <td><a href="{job['url']}">View Job</a></td>
              </tr>
            """
            
        html += """
            </table>
            <p>Date found: {}</p>
          </body>
        </html>
        """.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Attach HTML content
        msg.attach(MIMEText(html, 'html'))
        
        # Connect to server and send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        
        print(f"Email notification sent for {len(matching_jobs)} matching jobs")
        return True
    except Exception as e:
        print(f"Error sending email notification: {e}")
        return False

# Modified company matching code
matching_jobs = []  # To store all matching jobs

for record in json_data:
    print(record)
    if "email_sent" in record:
        continue
    if "company" not in record or "url" not in record:
        print("Warning: Record missing company or URL field:", record)
        continue
    
    # Skip if email already sent for this job
    if record.get("email_sent", False):
        print(f"Skipping {record['company']} - Email already sent")
        continue

    
    company_name = record["company"]
    job_url = record["url"]
    print(f"\nProcessing company: {company_name}")
    
    # Create a list of all company names to compare
    all_companies = [company_name] + excel_data['Employer (Petitioner) Name'].tolist()
    
    # Initialize TF-IDF Vectorizer
    vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 3))
    
    # Compute TF-IDF matrix
    tfidf_matrix = vectorizer.fit_transform(all_companies)
    
    # Calculate cosine similarity
    cosine_similarities = np.dot(tfidf_matrix[0:1], tfidf_matrix[1:].T).toarray()[0]
    
    # Add similarity scores to a copy of the dataframe
    result_df = excel_data.copy()
    result_df['Similarity_Score'] = cosine_similarities
    
    # Set a threshold for considering a match
    threshold = 0.6
    result_df['Is_Match'] = result_df['Similarity_Score'] >= threshold
    
    # Get matches above threshold
    matches = result_df[result_df['Is_Match']]
    
    # If there's at least one match, add to our matching jobs list
    if len(matches) > 0:
        best_match = matches.sort_values(by='Similarity_Score', ascending=False).iloc[0]
        print(f"Found match: {company_name} -> {best_match['Employer (Petitioner) Name']} (Score: {best_match['Similarity_Score']:.2f})")
        
        # Add to matching jobs list with all necessary info
        matching_jobs.append({
            'title': record.get('title', 'Unknown Title'),
            'company': company_name,
            'matched_company': best_match['Employer (Petitioner) Name'],
            'match_score': best_match['Similarity_Score'],
            'url': job_url,
            'location': record.get('location', 'Unknown Location')
        })

        print(matching_jobs)

        # Mark this record for email sent flag (will be updated later)
        record["email_sent"] = True

# After processing all jobs, send a single email if we have matches
if matching_jobs:
    recipient_email = os.environ.get("RECIPIENT_EMAIL")  # Replace with recipient's email
    if send_batch_email_notification(matching_jobs, recipient_email):
        with open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=4)
            print(f"Updated job database with email sent flags")
    else:
        print("Failed to send email, not updating email_sent flags")
else:
    print("No matching companies found, no email sent")
