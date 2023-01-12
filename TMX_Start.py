from splinter import Browser
from selenium import webdriver
import time
import datetime
import awswrangler.secretsmanager as sm
import os
import glob
import boto3
from botocore.exceptions import ClientError
import snowflake.connector
from sqlqueries import SqlQueries

# ------------- Check if it's a skip day (Job only runs a day after a business day) ------------- 
conn = snowflake.connector.connect(
                user=sm.get_secret_json('ficsnowflakeml').get('user'),
                password=sm.get_secret_json('ficsnowflakeml').get('pwd'),
                account='filcanfic.ca-central-1.privatelink'
                )

df = conn.cursor().execute(SqlQueries.query_skip_days).fetch_pandas_all()
today = datetime.date.today()

if today in df['SKIP_DAY'].to_list():
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{t}: Today is a skip day. Job will exit now.")
    exit()
else:
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{t}: Starting")


# ------------- set variables ------------- 
user = sm.get_secret_json('fictmxgrapevine').get('user2')
pwd = sm.get_secret_json('fictmxgrapevine').get('pwd')
notebook_id = sm.get_secret_json('fictmxgrapevine').get('tmx01_url_id')
url_notebook = f'https://fidelityinvestments.tmxgrapevine.com/#/notebook/{notebook_id}'
url_cluster=('https://hub.tmxanalytics.com/grapevine/clusters')
#url_login = 'https://apps.tmxanalytics.com/'
url_login = 'https://hub.tmxanalytics.com/login'
download_path = os.getcwd()
filename = "data_TMX.csv"
bucket = os.getenv('BUCKET_NAME')
bucket_path = os.getenv('BUCKET_PATH') # path includes workload name i.e FIC_ETL_TMX01/output/
object = str(bucket_path) + str(filename)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')

executable_path = {'executable_path':'/usr/local/bin/chromedriver'}
chrome_options.add_argument('--disable-dev-shm-usage')  

browser = Browser('chrome',**executable_path, headless=True, options=chrome_options)

cluster_name = 'fidelityinvestments.tmxgrapevine.com'


# ------------- Remove existing files ------------- 
def remove_old_files():
    old_files = glob.glob(f"{download_path}/data*.csv")  
    # Iterate over the list of filepaths & remove each file.
    for filePath in old_files:
        try:
            os.remove(filePath)
        except:
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"{t}: Error while deleting file ", filePath)

# ------------- Login to Grapevine ------------- 
def login_to_grapevine():
    browser.visit(url_login)
    time.sleep(25)
    browser.find_by_tag('input')[3].fill(user)
    browser.find_by_tag('input')[4].fill(pwd)
    #javascript = "document.getElementById('sign-in').click()"
    javascript = "document.getElementsByClassName('btn btn-info next_button')[0].click()"
    browser.execute_script(javascript)
    time.sleep(25)
    browser.visit(url_cluster)
    time.sleep(25)
    
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{t}: Logged into Grapevine')

# ------------- Start Cluster ------------- 
def start_cluster():    
    javascript_check_cluster_status = f"Array.from(document.getElementsByTagName('td')).filter(e => e.textContent == '{cluster_name}')[0].parentElement.children[1].textContent"
    
    if browser.evaluate_script(javascript_check_cluster_status) == 'TERMINATED':

        javascript_start_cluster = f"Array.from(document.getElementsByTagName('a')).filter( e=> (e.attributes['alt'] && e.attributes['alt'].textContent =='Start Cluster') && e.parentElement.parentElement.parentElement.children[0].textContent=='{cluster_name}' )[0].click()"
        browser.execute_script(javascript_start_cluster)
        time.sleep(5) # wait 5 seconds for modal to load
        javascript_confirm_start = f"Array.from(document.getElementsByTagName('button')).filter(e=> (e.textContent == 'Yes'))[0].click()"
        browser.execute_script(javascript_confirm_start)
        time.sleep(5) # wait 5 seconds for modal to load

    start_time = time.time()
    cluster_started = False
    while not cluster_started:
        if browser.evaluate_script(javascript_check_cluster_status)=='WAITING':
            cluster_started = True
            #wait some time, just in case
            time.sleep(60) #time to settle..
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f'{t}: Started the cluster')       
        else:
            time.sleep(5) #wait 5 secs before attempting to get
            if time.time() - start_time > 2000:
                break #exit the while loop if too long..

# ------------- Launch Notebook when cluster is running ------------- 
def launch_notebook():
    
    # go to notebook and run 
    browser.visit(url_notebook)
    time.sleep(30)
    
    # login again
    try:
        browser.find_by_tag('input')[4].fill(user)
        browser.find_by_tag('input')[5].fill(pwd)
        javascript = "document.getElementsByTagName('button')[10].click()"
        browser.execute_script(javascript)
        time.sleep(30)
    except Exception as e:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f'{t}: Already logged in: \n Exception:= {e}')
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{t}: Launched notebook')
    
# ------------- Go to notebook URL and run ------------- 
def run_notebook():
    browser.visit(url_notebook)
    time.sleep(30)

    # remove interpreter settings 
    try:
        javascript = "Array.from(document.getElementsByTagName('button')).filter(e => e.textContent =='Save')[0].click()"
        browser.execute_script(javascript)
        time.sleep(30)
    except Exception as e:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f'{t}: Already saved binding: \n Exception:= {e}')        

    #run all cells
    javascript = "Array.from(document.getElementsByTagName('button')).filter(e => e.getAttribute('uib-tooltip') == 'Run all paragraphs')[0].click()"
    browser.execute_script(javascript)
    time.sleep(30)        
        
    #confirm running all cells
    javascript= "Array.from(document.getElementsByTagName('button')).filter(e => e.textContent =='OK')[0].click()"
    browser.execute_script(javascript)

    # wait 5 minutes until script ends
    time.sleep(300) 
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{t}: Ran notebook')

# ------------- Download results ------------- 
def download_result():
    start_time = time.time()
    download_started = False
    
    #Checks if 'Download Data as CSV' button is available
    while not download_started:
        try:
            javascript = "Array.from(document.getElementsByTagName('button')).filter(e => e.getAttribute('uib-tooltip') == 'Download Data as CSV')[0].innerHTML"
            found_html = browser.evaluate_script(javascript)
        except Exception as e:
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f'{t}: Download button not found: \n Exception:= {e}')            
            found_html = 'not found'

        if "fa-download" in found_html:
            download_started = True
            #wait some time, just in case
            time.sleep(5)
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f'{t}: Success download started')
        else:
            time.sleep(5) # wait 5 secs before attempting to get
            if time.time() - start_time > 2000:
                break #exit the while loop if too long..

    #Reset the download behavior, incase session times out
    browser.driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_path}}
    command_result = browser.driver.execute("send_command", params)
    time.sleep(15)

    #Clicks 'Download Data as CSV' button
    javascript = "Array.from(document.getElementsByTagName('button')).filter(e => e.getAttribute('uib-tooltip') == 'Download Data as CSV')[0].click()"
    browser.execute_script(javascript)
    time.sleep(60)
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{t}: Downloaded results in {download_path}')

# ------------- Rename file ------------- 
def rename_file():
    file = glob.glob(f"{download_path}/data*.csv")
    os.rename(file[0],f"{download_path}/{filename}")

# ------------- Upload file to S3 ------------- 
def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f'{t}: Uploaded file to s3:{bucket}')
    except ClientError as e:
        print(error(e))
        return False
    return True
      
# ------------- ShutDown the cluster ------------- 
def shutdown():
    browser = Browser('chrome',**executable_path, headless=True, options=chrome_options)  
    browser.visit(url_login)
    time.sleep(25)
    browser.find_by_tag('input')[3].fill(user)
    browser.find_by_tag('input')[4].fill(pwd)
    #javascript = "document.getElementById('sign-in').click()"
    javascript = "document.getElementsByClassName('btn btn-info next_button')[0].click()"
    browser.execute_script(javascript)
    time.sleep(25)
    cluster_url=('https://hub.tmxanalytics.com/grapevine/clusters')
    browser.visit(cluster_url)

    time.sleep(25)
    #Finds row with cluster_name and clicks Stop Cluster button
    javascript_shutdown_cluster = f"Array.from(document.getElementsByTagName('a')).filter( e=> (e.attributes['alt'] && e.attributes['alt'].textContent =='Stop Cluster') && e.parentElement.parentElement.parentElement.children[0].textContent=='{cluster_name}' )[0].click()"
    browser.execute_script(javascript_shutdown_cluster)
    time.sleep(5) # wait 5 seconds for modal to load
    javascript_confirm_start = f"Array.from(document.getElementsByTagName('button')).filter(e=> (e.textContent == 'Yes'))[0].click()"
    browser.execute_script(javascript_confirm_start)
    time.sleep(5) # wait 5 seconds for modal to load   
    
    time.sleep(25)
    
    
    browser.quit()
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f'{t}: Shutting down notebook')
    
    return


if __name__ == '__main__':
    remove_old_files()
    login_to_grapevine()
    start_cluster()
    launch_notebook()
    run_notebook()
    download_result()
    rename_file()
    upload_to_s3(filename, bucket, object)
    shutdown()