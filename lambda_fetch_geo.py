
import json
import csv
import requests
import boto3
import os

# Initialize S3 client
s3_client = boto3.client('s3')

# Get environment variables
API_KEY = os.environ['OPENCAGE_API_KEY']  # OpenCage API Key
BUCKET_NAME = os.environ['BUCKET_NAME']    # S3 bucket name

# List of country names

COUNTRIES = [
    'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia',
    'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium',
    'Belize', 'Benin', 'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria',
    'Burkina Faso', 'Burundi', 'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada', 'Central African Republic', 'Chad',
    'Chile', 'China', 'Colombia', 'Comoros', 'Congo (Congo-Brazzaville)', 'Congo (Democratic Republic)', 'Costa Rica',
    'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'East Timor',
    'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Fiji', 'Finland',
    'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau',
    'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
    'Ivory Coast', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia',
    'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia',
    'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 'Monaco',
    'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar (Burma)', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Zealand',
    'Nicaragua', 'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Panama', 'Papua New Guinea',
    'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Kitts and Nevis', 'Saint Lucia',
    'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles',
    'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Korea', 'South Sudan', 'Spain',
    'Sri Lanka', 'Sudan', 'Suriname', 'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Togo', 'Tonga',
    'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom',
    'United States', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Vatican City', 'Venezuela', 'Vietnam', 'Yemen', 'Zambia', 'Zimbabwe'
]


def get_lat_long_for_country(country):
    url = f"""https://api.opencagedata.com/geocode/v1/json?q={
        country}&key={API_KEY}"""
    try:
        response = requests.get(url)
        data = response.json()
        if data['results']:
            result = data['results'][0]
            lat = result['geometry']['lat']
            lng = result['geometry']['lng']
            return {
                'Country': country,
                'Latitude': lat,
                'Longitude': lng
            }
        else:
            return {
                'Country': country,
                'Latitude': None,
                'Longitude': None
            }
    except Exception as e:
        print(f"Error fetching data for {country}: {e}")
        return {
            'Country': country,
            'Latitude': None,
            'Longitude': None
        }


def save_to_s3(csv_content, file_name, bucket_name):
    try:
        s3_client.put_object(
            Body=csv_content, Bucket=bucket_name, Key=file_name)
        print(f"""File {file_name} successfully uploaded to S3 bucket {
              bucket_name}.""")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def lambda_handler(event, context):
    # Fetch lat/long for each country
    results = [get_lat_long_for_country(country) for country in COUNTRIES]

    # Create a CSV file in memory
    csv_file_name = 'country_lat_long.csv'
    csv_content = ""

    with open('/tmp/' + csv_file_name, mode='w', newline='') as file:
        fieldnames = ['Country', 'Latitude', 'Longitude']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            writer.writerow(result)

    # Read the content of the CSV file to upload to S3
    with open('/tmp/' + csv_file_name, 'r') as file:
        csv_content = file.read()

    # Upload the CSV file to S3
    save_to_s3(csv_content, csv_file_name, BUCKET_NAME)

    return {
        'statusCode': 200,
        'body': json.dumps(f'CSV file {csv_file_name} uploaded to {BUCKET_NAME}')
    }
