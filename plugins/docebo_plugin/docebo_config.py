from airflow import DAG
from airflow.models import Variable

ENV_LANDING_DB= 'LANDING_DB'
ENV_DOCEBO_S3_INTEGRATION = 'DOCEBO_S3_INTEGRATION'


#1) AWS Cred and respective S3 details
aws_access_key_id = Variable.get("s3_access_key_id")
aws_secret_access_key = Variable.get("s3_secret_access_key")
region_name = 'us-east-1'
bucket_name = 'ot-datateam-docebo-intg'
s3_stage='LANDING_FILES'
source_folder_path = Variable.get("docebo_landing_path")
destination_base_folder_path = Variable.get("docebo_archive_path")
client_id = Variable.get("docebo_api_client_id")
client_secret = Variable.get("docebo_api_client_secret")
grant_type = "password"
report_name="User_Awards_API"
scope = "api"
user_api_name="user"
course_api_name="courses"
enrollment_api_name="enrollments"
report_api_name="reports"
username = Variable.get("docebo_username")
password = Variable.get("docebo_password")
token_url = "https://onetrustlearning.docebosaas.com/oauth2/token"
s3_bucket = "ot-datateam-docebo-intg"
s3_secret_access_key = Variable.get("s3_secret_access_key")
s3_access_key_id = Variable.get("s3_access_key_id")
s3_file_path = Variable.get("docebo_landing_path")
s3_region = "us-east-1"
end_points = [
        "https://onetrustlearning.docebosaas.com",
        "https://onetrustlearning.docebosaas.com/manage/v1/user",
        "https://onetrustlearning.docebosaas.com/course/v1/courses",
        "https://onetrustlearning.docebosaas.com/course/v1/courses/enrollments"
    ]



#2) file format Configuration
s3_file_config_report = ''' FILE_FORMAT = (type = 'csv' SKIP_HEADER = 1 FIELD_DELIMITER = ',' error_on_column_count_mismatch=false field_optionally_enclosed_by = '"' ) FORCE = TRUE ON_ERROR = 'continue' '''

s3_file_config = ''' FILE_FORMAT = (type = 'json' ) FORCE = TRUE ON_ERROR = 'continue' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE '''

#

enrollments_fields_columns = ''' 
	        user_id INTEGER,
            username VARCHAR,
            course_id INTEGER,
            course_uid VARCHAR,
            course_code VARCHAR,
            course_name VARCHAR,
            course_type VARCHAR,
            course_begin_date VARCHAR,
            course_end_date VARCHAR,
            enrollment_level VARCHAR,
            enrollment_status VARCHAR,
            enrollment_created_by VARCHAR,
            enrollment_created_at VARCHAR,
            enrollment_validity_begin_date VARCHAR,
            enrollment_validity_end_date VARCHAR,
            enrollment_date_last_updated VARCHAR,
            enrollment_completion_date VARCHAR,
            enrollment_score FLOAT

	''' 

users_fields_columns = ''' 
	        user_id     integer ,
            username	varchar ,
            first_name	varchar ,
            last_name	varchar ,
            email	varchar ,
            uuid	varchar ,
            is_manager	boolean,
            fullname	varchar ,
            last_access_date	varchar ,
            last_update	varchar ,
            creation_date	varchar ,
            status	varchar ,
            avatar	varchar ,
            language	varchar ,
            lang_code	varchar ,
            expiration_date	varchar ,
            level	varchar ,
            email_validation_status	varchar ,
            send_notification	varchar ,
            newsletter_optout	varchar ,
            newsletter_optout_date	varchar ,
            encoded_username	varchar ,
            timezone	varchar ,
            date_format	varchar ,
            field_1	varchar ,
            field_2	varchar ,
            field_3	varchar ,
            field_4	varchar ,
            field_5	varchar ,
            field_7	varchar ,
            field_8	varchar ,
            field_9	varchar ,
            field_10	varchar ,
            field_11	varchar ,
            field_12	varchar ,
            field_13	varchar ,
            field_15	varchar ,
            field_16	varchar ,
            field_17	varchar ,
            field_18	varchar ,
            field_19	varchar ,
            field_20	varchar ,
            field_21	varchar ,
            field_22	varchar ,
            field_23	varchar ,
            field_24	varchar ,
            field_25	varchar ,
            field_26	varchar ,
            field_27	varchar ,
            field_28	varchar ,
            field_29	varchar ,
            field_30	varchar ,
            multidomains	varchar ,
            manager_names	varchar ,
            managers	varchar ,
            active_subordinates_count	varchar ,
            actions	varchar ,
            expired	boolean

	''' 

courses_columns = ''' 
	        id	INTEGER,
            code	VARCHAR,
            uidCourse	VARCHAR,
            title	VARCHAR,
            type	VARCHAR,
            language	VARCHAR,
            language_label	VARCHAR,
            is_deleted	BOOLEAN,
            thumbnail	VARCHAR,
            course_status	VARCHAR,
            creation_date	TIMESTAMP_NTZ,
            published	BOOLEAN,
            category_name	VARCHAR,
            users_without_session_count	INTEGER,
            decommissioned_at	VARCHAR,
            removed_at	VARCHAR,
            last_update	TIMESTAMP_NTZ,
            created_by	VARCHAR,
            field_1	VARCHAR,
            outdated_count	INTEGER,
            imported_from_content_marketplace	VARCHAR,
            average_completion_time	FLOAT,
            waiting_list	INTEGER,
            enrolled_count	INTEGER,
            slug_name	VARCHAR,
            actions	VARCHAR,
            start_date	TIMESTAMP_NTZ,
            sessions_count	INTEGER,
            session_waiting_list	INTEGER,
            end_date	TIMESTAMP_NTZ

	''' 
	
certification_columns = ''' 
            username VARCHAR,
            employee_id VARCHAR,
            full_name VARCHAR,
            partner_account VARCHAR,
            partner_id VARCHAR,
            sfdc_id VARCHAR,
            training_date DATE,
            user_unique_id VARCHAR,
            certification_title VARCHAR,
            certification_code VARCHAR,
            certification_description VARCHAR,
            certification_duration VARCHAR,  
            certification_status VARCHAR,
            completed_activity VARCHAR,
            issued_on DATE,
            to_renew_in DATE
	''' 



s3_unload_list_dict = [
                        {
                            "s3_stage": s3_stage,
                            "s3_file_path": s3_file_path,
                            "s3_file_name": f"{user_api_name}.json",
                            "db_name": ENV_LANDING_DB,
                            "schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "table_name": "USERS",
                            "s3_db_name": ENV_LANDING_DB,
                            "s3_schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "s3_stage": s3_stage,
                            "columns": users_fields_columns,
                            "s3_file_config": s3_file_config,
                        },
                        {
                            "s3_stage": s3_stage,
                            "s3_file_path": s3_file_path,
                            "s3_file_name": f"{course_api_name}.json",
                            "db_name": ENV_LANDING_DB,
                            "schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "table_name": "Courses",
                            "s3_db_name": ENV_LANDING_DB,
                            "s3_schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "s3_stage": s3_stage,
                            "columns": courses_columns,
                            "s3_file_config": s3_file_config,
                        },
                        {
                            "s3_stage": s3_stage,
                            "s3_file_path": s3_file_path,
                            "s3_file_name": f"{report_api_name}.csv",
                            "db_name": ENV_LANDING_DB,
                            "schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "table_name": "USERS_CERTIFICATION",
                            "s3_db_name": ENV_LANDING_DB,
                            "s3_schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "s3_stage": s3_stage,
                            "columns": certification_columns,
                            "s3_file_config": s3_file_config_report,
                        },
                        {
                            "s3_stage": s3_stage,
                            "s3_file_path": s3_file_path,
                            "s3_file_name": f"{enrollment_api_name}.json",
                            "db_name": ENV_LANDING_DB,
                            "schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "table_name": "enrollments",
                            "s3_db_name": ENV_LANDING_DB,
                            "s3_schema_name": ENV_DOCEBO_S3_INTEGRATION,
                            "s3_stage": s3_stage,
                            "columns": enrollments_fields_columns,
                            "s3_file_config": s3_file_config,
                        }
                    ]

#files to move
files_to_move = []
for details in s3_unload_list_dict:
    files_to_move.append(details["s3_file_name"])


#ii) file_archival kwargs
fa_op_kwargs = {
    'aws_access_key_id': aws_access_key_id,
    'aws_secret_access_key': aws_secret_access_key,
    'region_name': region_name,
    'bucket_name': bucket_name,
    'source_folder_path': source_folder_path,
    'destination_base_folder_path': destination_base_folder_path,
    'files_to_move': files_to_move,
    'delete_flag': 'Y'
    }
