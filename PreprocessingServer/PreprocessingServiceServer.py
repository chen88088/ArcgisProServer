
from fastapi import FastAPI, HTTPException, Request
from contextlib import asynccontextmanager
import logging
import requests
import subprocess
from pydantic import BaseModel
from typing import List, Any, Dict
from pathlib import Path
import os
import shutil
import git
from DVCManager import DVCManager
from DVCWorker import DVCWorker
from typing import Dict
from LoggerManager import LoggerManager
from DagManager import DagManager
import config
import redis
import socket
import json


logging.basicConfig(level=logging.DEBUG)

# NODE_AGENT_URL = "http://10.52.52.137:8080"

# # SERVICE_NAME 
# SERVICE_NAME = "Preprocessing"
# SERVICE_ADDRESS = "http://10.52.52.137"
# SERVICE_PORT = 8081
# SERVICE_RESOURCE_TYPE = "ARCGIS"


CONSUL_HOST = "http://10.52.52.142:30850"

SERVICE_NAME = "Preprocessing"
NODE_ID = socket.gethostname()
SERVICE_PORT = 8003  # 本機服務端口
NODE_IP = "10.52.52.136"


#  掛載路徑 (本地測試用)
STORAGE_PATH = "F:/mnt/storage" 

# # service server 註冊模型
# class RegisterServiceRequest(BaseModel):
#     service_name: str
#     service_address: str
#     service_port: int
#     service_resource_type: str  # 例如: "CPU" 或 "GPU"

# 定義 Pydantic 模型
class DagRequest(BaseModel):
    DAG_ID: str
    EXECUTION_ID: str
    TASK_STAGE_TYPE: str
    DATASET_NAME: str
    DATASET_VERSION: str
    CODE_REPO_URL: Dict[str, str]
    IMAGE_NAME: Dict[str, str]
    MODEL_NAME: str
    MODEL_VERSION: str
    DEPLOYER_NAME: str
    DEPLOYER_EMAIL: str
    PIPELINE_CONFIG: Dict[str, Any]


def register_machine():
    """
    當 Preprocessing Server 啟動時，向 Consul 註冊
    """
    service_id = f"{NODE_ID}-{SERVICE_NAME}"
    
    response = requests.put(f"{CONSUL_HOST}/v1/agent/service/deregister/{service_id}")
    service_registration = {
        "ID": service_id,  # 用 Hostname 當作 Consul 記錄的機器 ID
        "Name": SERVICE_NAME,
        "Tags": [f"node_id={NODE_ID}", f"service_name={SERVICE_NAME}"],  # 加入標籤，讓 DAG 之後可以查詢
        "Address": NODE_IP,
        "Port": SERVICE_PORT,
        "Check": {
            "HTTP": f"http://{NODE_IP}:{SERVICE_PORT}/health",
            "Interval": "10s",
            "Timeout": "5s"
        }
    }

    # 向 Consul 註冊機器
    response = requests.put(f"{CONSUL_HOST}/v1/agent/service/register", data=json.dumps(service_registration))
    
    if response.status_code == 200:
        logging.info(f"機器 {NODE_ID} 成功註冊到 Consul！")
    else:
        logging.error(f"註冊失敗: {response.text}")

def deregister_machine():
    """
    當 Preprocessing Server 停止時，從 Consul 註銷
    """
    service_id = f"{NODE_ID}-{SERVICE_NAME}"
    
    response = requests.put(f"{CONSUL_HOST}/v1/agent/service/deregister/{service_id}")

    if response.status_code == 200:
        logging.info(f"✅ 機器 {NODE_ID} 已從 Consul 註銷")
    else:
        logging.error(f"❌ 註銷失敗: {response.text}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.debug("Entering lifespan startup")
    '''
    # register_data = RegisterServiceRequest(
    #     service_name=SERVICE_NAME,
    #     service_address=SERVICE_ADDRESS,
    #     service_port=SERVICE_PORT,
    #     service_resource_type=SERVICE_RESOURCE_TYPE, 
    # )
    '''
    try:
        '''
        #去跟servermanager註冊自己的ip與資源用量
        # logging.debug("Sending registration request to Server Manager")
        # response = requests.post(f"{NODE_AGENT_URL}/service/register", json=register_data.dict())
        # logging.debug(f"Registration request response status: {response.status_code}")
        # response.raise_for_status()
        # logging.debug(f"Response: {response.text}")
        # logging.info("Registered with Server Manager successfully")      
        '''
        # 啟動時執行
        register_machine()
        
    except requests.RequestException as e:
        logging.error(f"Failed to register with controller: {e}")
        logging.debug(f"Response: {e.response.text if e.response else 'No response'}")

    yield

    logging.debug("Entering lifespan shutdown")
    try:
        '''
        # #　去跟servermanager 清除自己的ip與資料
        # response = requests.post(f"{NODE_AGENT_URL}/service/unregister", json=register_data.dict())
        # response.raise_for_status()
        logging.info("Resources cleaned up successfully")'
        '''
        # 停止時執行
        deregister_machine()
    except requests.RequestException as e:
        logging.error(f"Failed to clean up resources: {e}")

app = FastAPI(lifespan=lifespan)

# 實例化 LoggerManager
logger_manager = LoggerManager()
# 實例化 DVCManger
dvc_manager = DVCManager(logger_manager)
# 實例化 DagManger
dag_manager = DagManager(logger_manager)


# # 連接 Redis（如果 Redis 是在 Docker 內部，用 `my-redis`，如果 Redis 在本機，用 `localhost`）
# # REDIS_HOST = "http://172.17.0.2"
# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
# redis_lock = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# try:
#     redis_lock = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
#     redis_lock.ping()  # 檢查 Redis 是否可用
# except redis.ConnectionError:
#     raise Exception("無法連線到 Redis，請確認 Redis 伺服器是否正在運行！")


@app.get("/health")
def health_check():
    return {"status": "OK"}

# [Preprocessing/RegisterDag]
@app.post("/Preprocessing/RegisterDag")
async def register_dag_and_logger_and_dvc_worker(request: DagRequest):
    """
        每隻dag先來註冊，並生成專屬的logger與dvc worker
    """
    
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    
    # 確認 DAG 是否已在 dag_manager 中註冊
    if dag_manager.is_registered(dag_id, execution_id):
        return {"status": "success", "message": "DAG is already registered."}

    
    dag_root_folder_path = os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")


    # 登記 DAG 到 dag_manager
    dag_manager.register_dag(dag_id, execution_id , dag_root_folder_path)

    # 初始化並註冊 logger
    if not logger_manager.logger_exists(dag_id, execution_id):
        logger_manager.init_logger(dag_id, execution_id, dag_root_folder_path)

    # 初始化並註冊 DVCWorker
    git_local_repo_for_dvc = os.path.join(dag_root_folder_path, "GIT_LOCAL_REPO_FOR_DVC")
    
    dvc_manager.init_worker(dag_id=dag_id, execution_id=execution_id, git_repo_path=git_local_repo_for_dvc)

    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/RegisterDag")

    # **打印請求的 request body**
    request_dict = request.model_dump()
    logger.info(f"Received request body:\n{json.dumps(request_dict, indent=4)}")

    logger.info(f"DAG, Logger, and DVCWorker initialized for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}")

    return {"status": "success", "message": f"DAG, Logger, and DVCWorker initialized for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}"}

# [Preprocessing/DownloadDataset]
@app.post("/Preprocessing/DownloadDataset")
async def download_dataset(request:DagRequest):
    # 獲取 POST 請求的 JSON 數據 
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    
    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/DownloadDataset")

    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    dataset_name =  request.DATASET_NAME
    dataset_version = request.DATASET_VERSION
    
    try:

        # 1.根據dataset_name & dataset_version 去跟dataset management service 取得該資料及該版本詳細資訊 (dvc file url)
        dataset_management_url = config.DMS_SERVICE_URL  # 替換為你的服務 URL
        endpoint = f"{dataset_management_url}/datasets/{dataset_name}/versions/{dataset_version}"

        logger.info(f"Fetching dataset details from Dataset Management Service: {endpoint}")
        response = requests.get(endpoint)

        if response.status_code != 200:
            logger.error(f"Failed to fetch dataset details: {response.json().get('detail')}")
            raise HTTPException(status_code=400, detail="Failed to fetch dataset details.")

        dataset_info = response.json()
        dvc_file_repo_url = dataset_info.get("dvc_file_repo_url")
        if not dvc_file_repo_url:
            logger.error("Missing DVC file repository URL in dataset details.")
            raise HTTPException(status_code=400, detail="Missing DVC file repository URL in dataset details.")

        # 2. git clone {dvc file url} based on dag_root_folder_path
        # 確認桌面是否有創建 "dag_root_folder"
        
        dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
        # 組合路徑：/mnt/storage/{dag_id}_{execution_id}
    
        if not dag_root_folder.exists():
            raise HTTPException(status_code=404, detail="dag_root_folder was not created")
        logger.info(f"Confirmed existence of {dag_root_folder}")

        dataset_clone_path = dag_root_folder/ "Dataset"
        os.makedirs(dataset_clone_path, exist_ok=True)
        
        # git clone 資料集檔案
        git.Repo.clone_from(dvc_file_repo_url, dataset_clone_path)
        logger.info(f"Cloning DVC repository {dvc_file_repo_url} to {dataset_clone_path}")


        # 3. cd 進去資料夾 並且 dvc pull
        os.chdir(dataset_clone_path)
        logger.info("Running DVC pull...")
        subprocess.run(["dvc", "pull"], check=True)

        logger.info("Dataset download completed successfully.")
        return {"status": "success", "message": "Dataset downloaded successfully.", "path": dataset_clone_path}

    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred while executing system command: {e}")
        raise HTTPException(status_code=500, detail="Error occurred while downloading dataset.")

    except requests.RequestException as e:
        logger.error(f"Error occurred while communicating with Dataset Management Service: {e}")
        raise HTTPException(status_code=500, detail="Error occurred while communicating with Dataset Management Service.")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

# [Preprocessing/SetupFolder]
@app.post("/Preprocessing/SetupFolder")
async def setup_folders_for_preprocessing(request: DagRequest):
    # 獲取 POST 請求的 JSON 數據
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    task_stage_type = request.TASK_STAGE_TYPE

    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/SetupFolder")

    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    code_repo_url = request.CODE_REPO_URL.get(task_stage_type)

    try:
        logger.info(f"Received request with {dag_id}")

        # 1. 確認桌面是否創建 "dag_root_folder"
        dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
        if not dag_root_folder.exists():
            raise HTTPException(status_code=404, detail="dag_root_folder was not created")
        logger.info(f"Confirmed existence of {dag_root_folder}")

        # 2. 檢查GIT LOCAL REPO FOR DVC
        dvc_worker.ensure_git_repository()
        logger.info("Ensured Git repository for DVC")

        # 3. 在 dag_root_folder 内 git clone
        repo_preprocessing_path = dag_root_folder / "NCU-RSS-1.5-Preprocessing"
        git.Repo.clone_from(code_repo_url, repo_preprocessing_path)
        assert repo_preprocessing_path.exists(), "Repo_ preprocessing clone failed"
        logger.info(f"Cloned preprocessing repo to {repo_preprocessing_path}")
        
        return {"status": "success",  "message": f"Cloned preprocessing repo to {repo_preprocessing_path}"}

    except HTTPException as e:
        logger.error(f"HTTPException occurred: {str(e.detail)}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# [Preprocessing/ModifyPreprocessingConfig]
@app.post("/Preprocessing/ModifyPreprocessingConfig")
async def modify_preprocessing_config(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    dataset_name = request.DATASET_NAME
    dataset_version = request. DATASET_VERSION
    
    # 獲取 Logger
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/ModifyPreprocessingConfig")

    try:
        dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
        dag_root_folder_path = Path(dag_root_folder)
        repo_preprocessing_path = Path(dag_root_folder_path / "NCU-RSS-1.5-Preprocessing")
        config_path = repo_preprocessing_path / "config.py"

        logger.info(f"Modifying config at {config_path} with request")

        # 讀取並修改配置文件
        with open(config_path, "r", encoding='utf-8') as file:
            config_content = file.read()

        # 替換路徑為新的值
        root_dict = str(repo_preprocessing_path)
        config_content = config_content.replace(r'C:\Users\jay\Desktop\code\NCU-RSS-1.5-preprocessing', root_dict)
        
        # 獲取當前使用者名稱
        user_name = os.getlogin()

        # 動態設定 workspace
        workspace = fr"C:\Users\{user_name}\Documents\ArcGIS\Projects\PNGoutput\PNGoutput.gdb"

        # 靜態設定 workspace
        # workspace = r"C:\Users\chen88088\Documents\ArcGIS\Projects\PNGoutput\PNGoutput.gdb"
        config_content = config_content.replace(r'C:\Users\Jay\Documents\ArcGIS\Projects\PNGoutput\PNGoutput.gdb', workspace)
        
        Dataset_Path = Path(dag_root_folder_path / "Dataset" / f"{dataset_name}___{dataset_version}")
        TIF_Path =  str(Dataset_Path / "TIF").replace("\\", "/")
        SHP_Path= str(Dataset_Path / "SHP").replace("\\", "/")
        
        config_content = config_content.replace('TIF', TIF_Path)
        config_content = config_content.replace('SHP', SHP_Path)
        
        with open(config_path, "w", encoding='utf-8') as file:
            file.write(config_content)
        
        logger.info(f"Config file {config_path} modified successfully")

        # 檢查文件內容是否有跟預期匹配
        with open(config_path, "r", encoding='utf-8') as file:
            verified_content = file.read()
        
        assert TIF_Path in verified_content, "TIF_Path not updated"
        assert SHP_Path in verified_content, "SHP_Path not updated"
        assert root_dict in verified_content, "Root directory not updated"
        assert workspace in verified_content, "Workspace path not updated"
        
        logger.info(f"Config file {config_path} verification successful")
        logger.info(f"[preprocessing]ModifyPreprocessingConfig finished for dag: {dag_id}_{execution_id}   !!!")

    except Exception as e:
        logger.error(f"An error occurred during config modification: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Config modification failed: {str(e)}")

    return {"status": "success", "message": "Config modified successfully"}

# [Preprocessing/GenerateParcelUniqueId]
@app.post("/Preprocessing/GenerateParcelUniqueId")
async def execute_generate_parcel_unique_id(request: DagRequest):

    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    

    # 獲取對應logger 
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/GenerateParcelUniqueId")

    dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
    root_folder_path = Path(dag_root_folder)
    repo_preprocessing_path = Path(root_folder_path / "NCU-RSS-1.5-Preprocessing")
    generate_parcel_unique_id_script_path = Path(repo_preprocessing_path / "generate_parcel_unique_id.py")
    python_executable = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'

    try:
        logger.info(f"Executing script {generate_parcel_unique_id_script_path} with Python {python_executable}")

        # 使用 subprocess.run 執行 Python 腳本
        result = subprocess.run([python_executable, generate_parcel_unique_id_script_path], check=True, text=True, capture_output=True)      
        logger.info(f"generate_parcel_unique_id_script executed successfully. stdout: {result.stdout}, stderr: {result.stderr}")       
        logger.info(f"[preprocessing]Execute generate_parcel_unique_id finished for dag: {dag_id}_{execution_id}   !!!")
        return {
            "status": "success",
            "message": f"execute generate_parcel_unique_id script successfully!!!",
            "stdout": result.stdout,
            "stderr": result.stderr
        } 
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing script: {e.stderr}")
        return {
            "status": "error",
            "stdout": e.stdout,
            "stderr": e.stderr
        }, 500
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return {"message": str(e)}, 500

# [Preprocessing/GeneratePng]
@app.post("/Preprocessing/GeneratePng")
async def execute_generate_png(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    

    # 獲取對應的 Logger 
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/GeneratePng")

    dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
    root_folder_path = Path(dag_root_folder)
    repo_preprocessing_path = Path(root_folder_path / "NCU-RSS-1.5-Preprocessing")
    generate_png_script_path = Path(repo_preprocessing_path / "generate_png.py")
    python_executable = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'

    try:
        logger.info(f"Executing script {generate_png_script_path} with Python {python_executable}")

        # 使用 subprocess.run 執行python腳本
        result = subprocess.run([python_executable, generate_png_script_path], check=True, text=True, capture_output=True)      
        logger.info(f"generate_png_script_path_script executed successfully. stdout: {result.stdout}, stderr: {result.stderr}")       
        logger.info(f"[preprocessing]Execute generate_png_script_path finished for dag: {dag_id}_{execution_id}   !!!")
        return {
            "status": "success",
            "message": f"execute generate_png script successfully!!!",
            "stdout": result.stdout,
            "stderr": result.stderr
        } 
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing script: {e.stderr}")
        return {
            "status": "error",
            "stdout": e.stdout,
            "stderr": e.stderr
        }, 500
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return {"message": str(e)}, 500

# [Preprocessing/WriteGtFile]
@app.post("/Preprocessing/WriteGtFile")
async def execute_write_gt_file(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    

    # 獲取對應logger 
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/WriteGtFile")

    dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
    root_folder_path = Path(dag_root_folder)
    repo_preprocessing_path = Path(root_folder_path / "NCU-RSS-1.5-Preprocessing")
    write_gt_file_script_path = Path(repo_preprocessing_path / "write_gt_file.py")
    python_executable = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'

    try:
        logger.info(f"Executing script {write_gt_file_script_path} with Python {python_executable}")

        # 使用 subprocess.run 執行python腳本
        result = subprocess.run([python_executable, write_gt_file_script_path], check=True, text=True, capture_output=True)      
        logger.info(f"write_gt_file_script executed successfully. stdout: {result.stdout}, stderr: {result.stderr}")       
        logger.info(f"[preprocessing]Execute write_gt_file_script_path finished for dag: {dag_id}_{execution_id}   !!!")
        return {
            "status": "success",
            "message": f"execute write_gt_file script successfully!!!",
            "stdout": result.stdout,
            "stderr": result.stderr
        } 
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing script: {e.stderr}")
        return {
            "status": "error",
            "stdout": e.stdout,
            "stderr": e.stderr
        }, 500
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return {"message": str(e)}, 500

# [Preprocessing/UploadPreprocessingResult]
@app.post("/Preprocessing/UploadPreprocessingResult")
async def upload_preprocessing_result(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    deployer_name = request.DEPLOYER_NAME
    deployer_email = request.DEPLOYER_EMAIL
    
    # 獲取對應的 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/UploadPreprocessingResult")

    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    try:
        
        dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
        root_folder_path = Path(dag_root_folder)
        repo_preprocessing_path = root_folder_path / "NCU-RSS-1.5-Preprocessing"
        ######################
        # 定義相對路徑起始點
        result_folder = 'result'
        
        # 初始化本地dvc倉庫，並配置 MinIO as remote-storage
        dvc_repo = repo_preprocessing_path 
        logger.info(f"Initializing DVC repository at {dvc_repo}")
        
        init_result = dvc_worker.initialize_dvc(dvc_repo, task_stage_type)
        if init_result["status"] == "error":
            logger.error(f"Failed to initialize DVC: {init_result['message']}")
            raise HTTPException(status_code=500, detail=init_result["message"])

        # 使用 DVC 管理文件夾並且推送到 MinIO
        logger.info(f"Adding and pushing folder {result_folder} to DVC")
        add_and_push_mask_result = dvc_worker.add_and_push_data(
            folder_path=f"{dvc_repo}/{result_folder}",
            folder_name=result_folder,
            stage_type= task_stage_type
        )
        if add_and_push_mask_result["status"] == "error":
            logger.error(f"Failed to add and push {result_folder}: {add_and_push_mask_result['message']}")
            raise HTTPException(status_code=500, detail=add_and_push_mask_result["message"])


        # 提交所有更改並推送到 Git

        subprocess.run(["git", "config", "--global", "user.name", f"{deployer_name}"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", f"{deployer_email}"], check=True)       
        logger.info(f"Committing and pushing DVC changes for {result_folder} and {result_folder} to Git")
        git_commit_result = dvc_worker.git_add_commit_and_push(
            project_path=root_folder_path,
            message=f"Add and track folder {result_folder}  with DVC"
        )
        if git_commit_result["status"] == "error":
            logger.error(f"Failed to commit and push to Git: {git_commit_result['message']}")
            raise HTTPException(status_code=500, detail=git_commit_result["message"])

        logger.info(f"Successfully added and pushed {result_folder} to DVC and Git")

        # sucess 
        logger.info(f"[preprocessing]UploadPreprocessingResult finished for dag: {dag_id}_{execution_id}   !!!")

        return {"status": "success", "message": f"The folders '{result_folder}' have been added and pushed to DVC."}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

# [Preprocessing/UploadLogToS3]
@app.post("/Preprocessing/UploadLogToS3")
async def upload_log_to_s3(request: DagRequest):
    
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Preprocessing/UploadLogToS3")
    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    try:       
        # Log 檔案路徑
        dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 

        log_file_path = Path(dag_root_folder) / "LOGS" / f"{dag_id}_{execution_id}.txt"
        if not log_file_path.exists():
            raise HTTPException(status_code=404, detail=f"Log file {log_file_path} not found.")

        # 重新命名檔案
        renamed_log_filename = f"{dag_id}_{execution_id}_{task_stage_type}.txt"

        # 上傳到 MinIO
        target_path = f"{dag_id}_{execution_id}/logs/{renamed_log_filename}"

        logger.info(f"Uploading log file: {log_file_path} to MinIO at {target_path}")

        # 使用 DVCWorker 的 S3 客戶端上傳
        dvc_worker.s3_client.upload_file(
            Filename=str(log_file_path),
            Bucket=dvc_worker.minio_bucket,
            Key=target_path
        )

        logger.info("Log file uploaded successfully.")

        return {
            "status": "success",
            "message": f"Log file uploaded to MinIO at {target_path}"
        }

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}") 

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("MachineServer:app", host=MACHINE_IP, port=MACHINE_PORT, reload=True)



