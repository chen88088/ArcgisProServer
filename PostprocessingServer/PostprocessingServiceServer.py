
from fastapi import FastAPI, HTTPException, Request
from contextlib import asynccontextmanager
import logging
import requests
import subprocess
from pydantic import BaseModel
from typing import List, Any
from pathlib import Path
import os
import shutil
import git
from DVCManager import DVCManager
from DVCWorker import DVCWorker
from typing import Dict
from LoggerManager import LoggerManager
from DagManager import DagManager
import re
import socket
import json


# 設置基本log配置
logging.basicConfig(level=logging.DEBUG)


# NODE_AGENT_URL = "http://10.52.52.137:8080"

# # SERVICE_NAME 
# SERVICE_NAME = "Postprocessing"
# SERVICE_ADDRESS = "http://10.52.52.137"
# SERVICE_PORT = 8085
# SERVICE_RESOURCE_TYPE = "ARCGIS"

CONSUL_HOST = "http://10.52.52.142:30850"

SERVICE_NAME = "Postprocessing"
NODE_ID = socket.gethostname()
SERVICE_PORT = 8005  # 本機服務端口
NODE_IP = "10.52.52.136"


#  掛載路徑 (本地測試用)
STORAGE_PATH = "F:/mnt/storage" 

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
    register_data = RegisterServiceRequest(
        service_name=SERVICE_NAME,
        service_address=SERVICE_ADDRESS,
        service_port=SERVICE_PORT,
        service_resource_type=SERVICE_RESOURCE_TYPE, 
    )
    '''
    try:
        '''
        #　去跟servermanager註冊自己的ip與資源用量
        logging.debug("Sending registration request to Server Manager")
        response = requests.post(f"{NODE_AGENT_URL}/service/register", json=register_data.dict())
        logging.debug(f"Registration request response status: {response.status_code}")
        response.raise_for_status()
        logging.debug(f"Response: {response.text}")
        logging.info("Registered with Server Manager successfully")'
        '''      
        # 啟動時執行
        register_machine()
    except requests.RequestException as e:
        logging.error(f"Failed to register with Server Manager: {e}")
        logging.debug(f"Response: {e.response.text if e.response else 'No response'}")

    yield

    logging.debug("Entering lifespan shutdown")
    try:
        '''
        #　去跟servermanager 清除自己的ip與資料
        response = requests.post(f"{NODE_AGENT_URL}/service/unregister", json=register_data.dict())
        response.raise_for_status()
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

dag_manager = DagManager(logger_manager)

@app.get("/health")
def health_check():
    return {"status": "OK"}

# [Postprocessing/RegisterDag]
@app.post("/Postprocessing/RegisterDag")
async def register_dag_and_logger_and_dvc_worker(request: DagRequest):
    """dag先來註冊，並生成專屬的logger與dvc worker
    
    Args:
        request (Request): 請求本體

    Raises:
        HTTPException: _description_

    Returns:
        _type_: 回應狀態
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
        logger_manager.log_section_header(logger, "Postprocessing/RegisterDag")

    # **打印請求的 request body**
    request_dict = request.model_dump()
    logger.info(f"Received request body:\n{json.dumps(request_dict, indent=4)}")

    logger.info(f"DAG, Logger, and DVCWorker initialized for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}")

    return {"status": "success", "message": f"DAG, Logger, and DVCWorker initialized for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}"}

# [Postprocessing/SetupFolder]
@app.post("/Postprocessing/SetupFolder")
async def setup_folders_for_postprocessing(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

    task_stage_type = request.TASK_STAGE_TYPE
    code_repo_url = request.CODE_REPO_URL.get(task_stage_type)
    
    # 獲取或初始化 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Postprocessing/SetupFolder")

    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    try:
        logger.info(f"Received request with data: {dag_id}_{execution_id}")

        # 1. 確認桌面是否有創建 "dag_root_folder"
        dag_root_folder = Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
        if not dag_root_folder.exists():
            raise HTTPException(status_code=404, detail="dag_root_folder was not created")
        logger.info(f"Confirmed existence of {dag_root_folder}")

        # 2. 檢查GIT LOCAL REPO FOR DVC
        dvc_worker.ensure_git_repository()
        logger.info("Ensured Git repository for DVC")

        # 3. 在 "moa_inference_folder" 内創建 "NCU-RSS-Predict-Postprocessing"
        repo_postprocessing_path = dag_root_folder / "NCU-RSS-Predict-Postprocessing"
        git.Repo.clone_from(code_repo_url, repo_postprocessing_path)
        assert repo_postprocessing_path.exists(), "Repo_postprocessing clone failed"
        logger.info(f"Cloned postprocessing repo to {repo_postprocessing_path}")

        logger.info(f"[setup]SetupAllFoldersForPrediction finished for dag: {dag_id}_{execution_id}   !!!")

        return {"status": "success", "message": f"Postprocessing folder setup successfully {repo_postprocessing_path}"}

    except HTTPException as e:
        logger.error(f"HTTPException occurred: {str(e.detail)}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# [Postprocessing/DownloadInferenceOutputFiles]
@app.post("/Postprocessing/DownloadInferenceOutputFiles")
async def download_inference_outputfiles(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE
    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

    stage_type_for_download = "Inference"
    
    # 0.获取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Postprocessing/DownloadInferenceOutputFiles")
    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    #TODO: STRAT FROM HEHR!!!
    # 1.定義 DVC 文件名
    inference_output_filename = "model.dvc"

    # 2.創建資料夾，用於暫存拉取的 DVC 數據
    dag_root_folder = Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
    root_folder_path = Path(dag_root_folder)
    
    temp_download_folder =  root_folder_path/ "temp_postprocessing_download"
    temp_download_folder.mkdir(parents=True, exist_ok=True)

    try:
        # 3.拉取 inference_result數據 (=Model ; inference之產物資料夾 )
        logger.info(f"Pulling {inference_output_filename} into {temp_download_folder}")
        pull_result = dvc_worker.pull(stage_type_for_download, inference_output_filename, temp_download_folder)
        if pull_result["status"] == "error":
            logger.error(f"Failed to pull {inference_output_filename}: {pull_result['message']}")
            raise HTTPException(status_code=500, detail=f"Failed to pull NIRRG data: {pull_result['message']}")

        # 目標文件夾路徑
        postprocessing_folder_path = root_folder_path / "NCU-RSS-Predict-Postprocessing"
        dest_PRED_folder = postprocessing_folder_path / 'PRED'
        # 確保目標文件夾的父目錄存在
        dest_PRED_folder.mkdir(parents=True, exist_ok=True)
        os.makedirs(dest_PRED_folder.parent, exist_ok=True)

        # 如果目標目錄已经存在，先删除
        if dest_PRED_folder.exists():
            logger.info(f"Deleting existing folder {dest_PRED_folder}")
            shutil.rmtree(dest_PRED_folder)
            if dest_PRED_folder.exists():
                logger.error(f"Failed to delete {dest_PRED_folder}")
                raise HTTPException(status_code=500, detail=f"Failed to delete {dest_PRED_folder}")

        # 重命名目錄為目標目錄
        need_to_be_renamed_target_folder = temp_download_folder / 'Model'
        logger.info(f"Renaming {need_to_be_renamed_target_folder} to {dest_PRED_folder}")
        (need_to_be_renamed_target_folder).rename(dest_PRED_folder)
        if not dest_PRED_folder.exists():
            logger.error(f"Failed to rename folder to {dest_PRED_folder}")
            raise HTTPException(status_code=500, detail=f"Failed to rename folder to {dest_PRED_folder}")

        # 返回成功響應
        logger.info(f"Successfully downloaded and renamed INFERENCE OUTPUT FILES folders for DAG {dag_id}, execution {execution_id}")
        logger.info(f"[Postprocessing]DownloadInferenceOutputFiles finished for dag: {dag_id}_{execution_id}   !!!")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# [Postprocessing/ModifyPostprocessingConfig]
@app.post("/Postprocessing/ModifyPostprocessingConfig")
async def modify_postprocessing_config(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    dataset_name = request.DATASET_NAME
    dataset_version = request. DATASET_VERSION

    # 取得logger
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Postprocessing/ModifyPostprocessingConfig")

    try:
        dag_root_folder = Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
        dag_root_folder_path = Path(dag_root_folder)
        repo_postprocessing_path = Path(dag_root_folder_path / "NCU-RSS-Predict-Postprocessing")
        config_path = repo_postprocessing_path / "configs" / "config.py"

        logger.info(f"Modifying config at {config_path} with data: {dag_id}_{execution_id}")

        # 讀取並修改文件
        with open(config_path, "r", encoding='utf-8') as file:
            config_content = file.read()

        # 替換為新的值
        # [workspace]
        old_workspace = r"C:\Users\AOIpc\Documents\ArcGIS\Projects\MidTerm Report\MidTerm Report.gdb"
        new_workspace = r"C:\Users\PongeSheng\Documents\ArcGIS\Projects\PNGoutput\PNGoutput.gdb"
        config_content = config_content.replace(old_workspace, new_workspace)

        # [directory]
        old_directory = r"D:\RSS-1.5_code\NCU-RSS-Predict-Postprocessing"
        new_directory = str(repo_postprocessing_path)
        config_content = config_content.replace(old_directory, new_directory)

        # [Too_box]
        old_Tool_box = r"C:\Users\AOIpc\AppData\Local\Programs\ArcGIS\Pro\Resources\ArcToolBox\toolboxes\Conversion Tools.tbx"
        new_Tool_box = r"C:\Program Files\ArcGIS\Pro\Resources\ArcToolBox\toolboxes\Conversion Tools.tbx"
        config_content = config_content.replace(old_Tool_box, new_Tool_box)

        # [SHP_Path] 
        Dataset_Path = Path(dag_root_folder_path / "Dataset" / f"{dataset_name}___{dataset_version}")
        SHP_Path= str(Dataset_Path / "SHP").replace("\\", "/")
        
        # 使用正則表達式精确匹配 SHP_Path 賦值語句
        pattern_of_SHP_Path = r'(SHP_Path\s*=\s*r")(.*?)(")'
        config_content = re.sub(pattern_of_SHP_Path, r'\1' + SHP_Path + r'\3', config_content)
        
        with open(config_path, "w", encoding='utf-8') as file:
            file.write(config_content)
        
        logger.info(f"Config file {config_path} modified successfully")

        # 驗證文件内容
        with open(config_path, "r", encoding='utf-8') as file:
            verified_content = file.read()
        
        assert new_workspace in verified_content, "Workspace path not updated"
        assert new_directory in verified_content, "Root directory not updated"
        assert new_Tool_box in verified_content, "Tool_box Path not updated"
        assert SHP_Path in verified_content, "SHP_Path not updated"
        
        
        logger.info(f"Config file {config_path} verification successful")
        logger.info(f"[preprocessing]ModifyPreprocessingConfig finished for dag: {dag_id}_{execution_id}   !!!")

    except Exception as e:
        logger.error(f"An error occurred during config modification: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Config modification failed: {str(e)}")

    return {"status": "success", "message": "Config modified successfully"}

# [Postprocessing/ExecutePostprocessing]
@app.post("/Postprocessing/ExecutePostprocessing")
async def execute_postprocessing(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Postprocessing/ExecutePostprocessing")

    logger.info("Received request to execute postprocessing script")

    # 建構推理根文件夾路徑
    dag_root_folder = Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 
    root_folder_path = Path(dag_root_folder)

    # 建構 write_back_pred_to_shp.py 文件路徑
    script_directory = root_folder_path / "NCU-RSS-Predict-Postprocessing"
    write_back_pred_to_shp_script_path = script_directory / "write_back_pred_to_shp.py"
    python_executable = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

    logger.info(f"Script path: {write_back_pred_to_shp_script_path}")

    if not write_back_pred_to_shp_script_path.exists():
        logger.error("Script file not found")
        raise HTTPException(status_code=404, detail="Script file not found")

    try:
        # 使用 subprocess 運行腳本，並捕获输出
        result = subprocess.run(
            [python_executable, str(write_back_pred_to_shp_script_path)],
            capture_output=True,
            text=True,
            check=True,
            cwd=str(script_directory)  # 設置工作目錄為腳本目錄
        )

        # 將脚本輸出紀錄到日誌
        logger.info(f"Script output:\n{result.stdout}")
        if result.stderr:
            logger.error(f"Script errors:\n{result.stderr}")

        logger.info(f"[postprocessing]ExecutePostprocessing finished for dag: {dag_id}_{execution_id}   !!!")

        return {"status": "success", "output": result.stdout, "errors": result.stderr}
    except subprocess.CalledProcessError as e:
        logger.error(f"Script failed with error: {e.stderr}")
        raise HTTPException(status_code=500, detail=f"Script failed with error: {e.stderr}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# [Postprocessing/UploadLogToS3]
@app.post("/Postprocessing/UploadLogToS3")
async def upload_log_to_s3(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    stage_type = request.TASK_STAGE_TYPE

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
        
    # 獲取或初始化 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Postprocessing/UploadLogToS3")
    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    try:
        # Log 檔案路徑
        dag_root_folder =Path(os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")) 

        log_file_path =  dag_root_folder / "LOGS" / f"{dag_id}_{execution_id}.txt"
        if not log_file_path.exists():
            raise HTTPException(status_code=404, detail=f"Log file {log_file_path} not found.")

        # 重新命名檔案
        renamed_log_filename = f"{dag_id}_{execution_id}_{stage_type}.txt"

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



