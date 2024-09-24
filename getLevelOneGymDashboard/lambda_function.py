import pandas as pd
import json
import io
import base64
from requests_toolbelt.multipart import decoder

def lambda_handler(event, context):
    try:
        # Ensure the request is a POST request
        if event['httpMethod'] != 'POST':
            return {
                'statusCode': 405,
                'body': json.dumps({'message': 'Method Not Allowed'})
            }
        
        # Extract and validate Content-Type header
        content_type = event['headers'].get('Content-Type') or event['headers'].get('content-type')
        if not content_type:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Content-Type header is missing'})
            }
        
        # Decode the base64 encoded body
        body = base64.b64decode(event['body'])

        # Use requests_toolbelt decoder to parse the multipart form-data
        multipart_data = decoder.MultipartDecoder(body, content_type)
        
        # Initialize a DataFrame to hold CSV data
        df = None

        # Loop through parts to find and process the CSV file
        for part in multipart_data.parts:
            content_disposition = part.headers.get(b'Content-Disposition').decode('utf-8')
            if 'file' in content_disposition:
                # Load CSV data into a Pandas DataFrame
                csv_content = part.content.decode('utf-8')
                csv_data = io.StringIO(csv_content)
                df = pd.read_csv(csv_data)
                break

        # Validate that we have successfully read a CSV file into the DataFrame
        if df is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'No CSV file found in the request'})
            }
        
        # Check for required columns
        required_columns = ['clientId', 'status', 'startDate', 'endDate', 'name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Missing required columns: {missing_columns}'})
            }
        
        # Convert date columns to datetime format and handle time zones
        df['startDate'] = pd.to_datetime(df['startDate'], utc=True).dt.tz_convert('America/Campo_Grande')
        df['endDate'] = pd.to_datetime(df['endDate'], utc=True).dt.tz_convert('America/Campo_Grande')

        # Calculate duration in minutes for each check-in
        df['duration_minutes'] = (df['endDate'] - df['startDate']).dt.total_seconds() / 60

        # Group by client and calculate summary statistics
        client_summary = df.groupby(['clientId', 'name']).agg(
            id=('clientId', 'max'),  # ID
            total_checkins=('id', 'count'),  # Count the number of check-ins per client
            average_duration=('duration_minutes', 'mean'),  # Calculate the mean duration of visits
            last_start_date=('startDate', 'max'),  # Last checkin
            status=('status', 'last')  # Status
        ).reset_index()
        
        # Convert the 'last_start_date' to string format to make it serializable
        client_summary['last_start_date'] = client_summary['last_start_date'].astype(str)

        # Calculate unique active and inactive clients
        unique_active_clients = df[df['status'] == 'active']['clientId'].nunique()
        unique_inactive_clients = df[df['status'] == 'inactive']['clientId'].nunique()

        # Extract day of the week and hour of the day for each check-in
        df['day_of_week'] = df['startDate'].dt.day_name()
        df['hour_of_day'] = df['startDate'].dt.hour

        # Group by day of the week and hour of day, then count check-ins
        checkins_by_day_time = df.groupby(['day_of_week', 'hour_of_day']).size().reset_index(name='checkin_count')
        
        # Calculate the average of the 'average_duration' column in the client_summary DataFrame
        average_duration_overall = client_summary['average_duration'].mean()
        
        # Calculate the average of the 'total_checkins' column in the client_summary DataFrame
        average_total_checkins_overall = client_summary['total_checkins'].mean()

        # Create a response dictionary
        response = {
            'client_summary': client_summary.to_dict(orient='records'),
            'checkins_by_day_time': checkins_by_day_time.to_dict(orient='records'),
            'unique_active_clients': unique_active_clients,
            'unique_inactive_clients': unique_inactive_clients,
            'average_duration_overall': average_duration_overall,
            'average_total_checkins_overall': average_total_checkins_overall,
            'message': 'CSV file processed successfully'
        }

        # Return the response with a success status code
        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }

    except pd.errors.ParserError:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Error parsing CSV file. Please check the file format.'})
        }
        
    except Exception as e:
        # General exception handling
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }
