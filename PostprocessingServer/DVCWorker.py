import subprocess
from pathlib import Path
import logging
import boto3
import subprocess
import shutil

class DVCWorker:
    def __init__(self, dag_id: str, execution_id: str, minio_bucket: str, minio_url: str, access_key: str, secret_key: str, git_repo_path: str, logger):
        self.dag_id = dag_id
        self.execution_id = execution_id
        self.git_repo_path = Path(git_repo_path).resolve()  # Git Local Path 儲存
        self.minio_bucket = minio_bucket
        self.minio_url = minio_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.remote_name = f"remote_{dag_id}_{execution_id}"
        self.s3_client = boto3.client('s3', endpoint_url=self.minio_url, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
        self.logger = logger

        # 檢查並創建 Git 儲存路徑
        self.create_directory_if_not_exists(self.git_repo_path)
        # 初始化 Git 本地倉庫
        self.ensure_git_repository()

        # # 配置遠程倉庫(選配)
        # self.configure_remote(self.git_repo_path)

    def create_directory_if_not_exists(self, path: Path):
        """檢查目錄是否存在，如果不存在則創建它"""
        path.resolve()
        if not path.exists():
            self.logger.info(f"Directory {path} does not exist. Creating it.")
            path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Directory {path} created.")
        else:
            self.logger.info(f"Directory {path} already exists.")
 
    def ensure_git_repository(self):
        """確保指定路徑是一个 Git 倉庫，並配置 Git 遠程倉庫"""
        git_dir = self.git_repo_path / ".git"
        
        if not git_dir.exists():
            self.logger.info(f"Directory {self.git_repo_path} is not a Git repository. Initializing a new Git repository.")
            subprocess.run(['git', 'init'], check=True, cwd=self.git_repo_path)
            self.logger.info(f"Initialized empty Git repository in {self.git_repo_path}")
        else:
            self.logger.info(f"Directory {self.git_repo_path} is already a Git repository.")

    def ensure_dvc_repository(self, folder_path: Path, stage_type:str):
        """確保指定路徑是一个 DVC 倉庫，使用 --no-scm 選項"""
        dvc_dir = folder_path / ".dvc"
        if not dvc_dir.exists():
            self.logger.info(f"Directory {folder_path} is not a DVC repository. Initializing a new DVC repository with --no-scm.")
            self.initialize_dvc(folder_path, stage_type)
            self.logger.info(f"Initialized empty DVC repository in {folder_path} with --no-scm")

    def configure_remote(self, project_path: Path, appointed_bucket:str, service_type:str):
        """配置 MinIO 作為指定路徑 DVC 倉庫的remote"""
        try:
            # 檢查並創建 MinIO bucket（如果不存在）
            if not self.bucket_exists(appointed_bucket):
                self.s3_client.create_bucket(Bucket=appointed_bucket)
                self.logger.info(f"Bucket {appointed_bucket} created")
            else:
                self.logger.info(f"Bucket {appointed_bucket} already exists")

            remote_path = f's3://{appointed_bucket}/{self.dag_id}_{self.execution_id}/{service_type}/dataset'
            subprocess.run(['dvc', 'remote', 'add', '-d', self.remote_name, remote_path, '--force'], check=True, cwd=project_path)
            subprocess.run(['dvc', 'remote', 'modify', self.remote_name, 'endpointurl', self.minio_url], check=True, cwd=project_path)
            subprocess.run(['dvc', 'remote', 'modify', self.remote_name, 'access_key_id', self.access_key], check=True, cwd=project_path)
            subprocess.run(['dvc', 'remote', 'modify', self.remote_name, 'secret_access_key', self.secret_key], check=True, cwd=project_path)
            subprocess.run(['dvc', 'remote', 'modify', self.remote_name, 'use_ssl', 'false'], check=True, cwd=project_path)
            self.logger.info(f"Configured MinIO {remote_path} as remote storage for DVC at {project_path}")
            return {"status": "success", "message": "DVC initialized and MinIO remote configured successfully."}
        except subprocess.CalledProcessError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def bucket_exists(self, bucket_name: str) -> bool:
        """檢查 MinIO bucket 是否存在"""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except self.s3_client.exceptions.NoSuchBucket:
            return False
        except Exception as e:
            self.logger.error(f"Error checking bucket existence: {str(e)}")
            return False

    def initialize_dvc(self, dvc_repo_path: str, stage_type:str):
        """初始化指定路徑的 DVC 倉庫，並配置 MinIO 作為remote storage"""
        dvc_repo_path = Path(dvc_repo_path).resolve()
        dvc_path = dvc_repo_path / ".dvc"
        service_type = stage_type

        if not dvc_path.exists():
            self.logger.debug(f"Initializing DVC repository at {dvc_repo_path}")
            subprocess.run(["dvc", "init", "--no-scm"], check=True, cwd=dvc_repo_path)

        # 確保 Git 倉庫已經初始化
        self.ensure_git_repository()

        # 配置 MinIO 作為remote storage
        appointed_data_bucket = self.minio_bucket
        config_result = self.configure_remote(dvc_repo_path, appointed_data_bucket, service_type)

        # # 将 .dvc 文件提交到 Git 中
        # dvc_files = list(dvc_repo_path.glob("*.dvc"))
        # if dvc_files:
        #     for dvc_file in dvc_files:
        #         subprocess.run(['cp', str(dvc_file), str(self.git_repo_path)], check=True)
        #     self.git_add_commit_and_push(self.git_repo_path, f"Initialize DVC at {dvc_repo_path}")

        return config_result
    
    def add(self, folder_path: str, folder_name: str , stage_type:str):
        """將指定文件夾(folder_path)添加到 DVC 管理中 並且複製紀錄檔至指定git repo"""
        folder_path = Path(folder_path).resolve()

        if not folder_path.exists():
            raise FileNotFoundError(f"The folder '{folder_path}' does not exist.")

        try:
            # 在當前文件夾路徑的上層目錄中執行 dvc add 操作，但只對指定的目錄執行操作
            subprocess.run(['dvc', 'add', str(folder_path)], check=True, cwd=folder_path.parent)
            self.logger.info(f"Added {folder_name} to DVC tracking.")

            # 生成的 .dvc 文件路徑
            dvc_file = Path(folder_path.parent / f"{folder_name}.dvc")
            if dvc_file.exists():               
                # 把產生的 .dvc檔 移動到指定的 repo [GIT_LOCAL_REPO_FOR_DVC/{dagid_exeid}/{stag_type}/產物資料夾]]
                appointed_dvc_file_storage_folder = Path(self.git_repo_path / stage_type /f'{stage_type}_{folder_name}' ).resolve()
                self.create_directory_if_not_exists(appointed_dvc_file_storage_folder)

                shutil.copy(str(dvc_file), str(appointed_dvc_file_storage_folder))

                # 配置的git repo資料夾 git add /git commit
                self.git_add_commit_and_push(self.git_repo_path, f"Add {folder_name} dataset DVC file")
            else:
                self.logger.error(f".dvc file not found for {folder_name}. Expected at {dvc_file}.")
                return {"status": "error", "message": f".dvc file not found for {folder_name}."}
            
            return {"status": "success", "message": f"Folder {folder_name} added to DVC tracking and DVC file committed to Git."}
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error adding folder to DVC: {str(e)}")
            return {"status": "error", "message": str(e)}

    def push(self, folder_path: str):
        """將指定 DVC 倉庫中的文件推送到remote storage"""
        folder_path = Path(folder_path).resolve()
        root_folder_path = Path(folder_path).parent.resolve()

        try:
            result = subprocess.run(['dvc', 'push'], check=True, text=True, capture_output=True, cwd=root_folder_path)
            self.logger.info("Pushed data to remote storage.")
            return {"status": "success", "stdout": result.stdout, "stderr": result.stderr}
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error pushing data to remote storage: Command '{e.cmd}' returned non-zero exit status {e.returncode}")
            self.logger.error(f"Detailed error output:\nSTDOUT:\n{e.stdout}\nSTDERR:\n{e.stderr}")
            return {"status": "error", "stdout": e.stdout, "stderr": e.stderr}
        except Exception as e:
            self.logger.error(f"Unexpected error occurred: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def upload_dvc_file_to_minio(self, dvc_file_path: Path, stage_type: str):
        """上傳 .dvc 文件到 MinIO """
        try:
            # 檢查 bucket 是否存在
            if not self.bucket_exists(self.minio_bucket):
                self.logger.info(f"Bucket '{self.minio_bucket}' does not exist. Creating it.")
                self.s3_client.create_bucket(Bucket=self.minio_bucket)
                self.logger.info(f"Bucket '{self.minio_bucket}' created.")

            target_path = f"{self.dag_id}_{self.execution_id}/{stage_type}/dvc_files/{dvc_file_path.name}"
            self.s3_client.upload_file(str(dvc_file_path), self.minio_bucket, target_path)
            self.logger.info(f"Uploaded .dvc file to MinIO at {self.minio_bucket}/{target_path}")
            return {"status": "success", "message": f"Uploaded .dvc file to MinIO at {self.minio_bucket}/{target_path}"}
        except Exception as e:
            self.logger.error(f"Failed to upload .dvc file to MinIO: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def add_and_push_data(self, folder_path: str, folder_name: str, stage_type: str):
        """將指定文件夾(folder path)添加到 DVC，推送到 MinIO， 並將 .dvc 文件提交到 Git"""
        folder_path = Path(folder_path).resolve()

        if not folder_path.exists():
            raise FileNotFoundError(f"The folder '{folder_path}' does not exist.")

        # DVC ADD
        add_result = self.add(folder_path, folder_name, stage_type)
        if add_result["status"] == "error":
            return add_result

        # DVC PUSH
        push_result = self.push(folder_path)
        if push_result["status"] == "error":
            return push_result

        # 上傳 .dvc 文件到 MinIO 同一個 bucket 的不同資料夾
        dvc_file = Path(folder_path.parent / f"{folder_name}.dvc")
        upload_result = self.upload_dvc_file_to_minio(dvc_file, stage_type)
        if upload_result["status"] == "error":
            return upload_result
        
        return {"status": "success", "message": "Data added to DVC, pushed to remote storage, and .dvc file uploaded to MinIO."}


    def git_add_commit_and_push(self, project_path: str, message: str):
        """將指定路徑中的 .dvc 文件複製到统一的 Git 倉庫中，並提交和推送"""
        try:
            # 在 Git 本地倉庫中添加 .dvc 文件
            subprocess.run(['git', 'add', '.'], check=True, cwd=self.git_repo_path)

            # 檢查 Git 倉庫中是否有變化需要提交
            result = subprocess.run(['git', 'status', '--porcelain'], check=True, text=True, capture_output=True, cwd=self.git_repo_path)
            if not result.stdout.strip():
                self.logger.info("No changes to commit in Git repository.")
                return {"status": "success", "message": "No changes to commit in Git repository."}

            # 提交更改到 Git 倉庫
            subprocess.run(['git', 'commit', '-m', message], check=True, cwd=self.git_repo_path)

            # 推送更改到remote storage（可選）
            # subprocess.run(['git', 'push'], check=True, cwd=self.git_repo_path)

            self.logger.info("Committed and (optionally) pushed DVC changes to Git repository.")
            return {"status": "success", "message": "Changes committed (and optionally pushed) to Git repository."}
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error committing (and pushing) to Git: {str(e)}")
            return {"status": "error", "message": str(e)}

    def download_dvc_file_from_minio(self, dvc_filename: str, target_path: Path, stage_type: str):
        """从 MinIO 下载 .dvc 文件"""
        try:
            # 在 MinIO 中找到對應的路徑
            minio_key = f"{stage_type}/{dvc_filename}"
            self.s3_client.download_file(self.minio_bucket, minio_key, str(target_path))
            self.logger.info(f"Downloaded {dvc_filename} from MinIO to {target_path}")
        except Exception as e:
            self.logger.error(f"Failed to download {dvc_filename} from MinIO: {str(e)}")
            raise FileNotFoundError(f"Failed to download {dvc_filename} from MinIO: {str(e)}")
        
    def pull(self, stage_type: str, dvc_filename: str, folder_path: str):
        """从 MinIO 拉取指定 .dvc 文件的内容到指定路徑，並根據.dvc 文件下載數據"""
        folder_path = Path(folder_path).resolve()
        
        try:
            # 確保指定路徑存在
            if not folder_path.exists():
                raise FileNotFoundError(f"Destination folder {folder_path} does not exist.")

            # 確保指定路徑存在一个 DVC Repo
            self.ensure_dvc_repository(folder_path, stage_type)

            # 從 MinIO 下載 .dvc 文件
            dvc_file_key = f"{self.dag_id}_{self.execution_id}/{stage_type}/dvc_files/{dvc_filename}"
            local_dvc_file_path = folder_path / dvc_filename
            
            try:
                self.logger.info(f"Downloading .dvc file from MinIO: {dvc_file_key} to {local_dvc_file_path}")
                self.s3_client.download_file(self.minio_bucket, dvc_file_key, str(local_dvc_file_path))
                self.logger.info(f"Downloaded {dvc_filename} to {local_dvc_file_path}")
            except Exception as e:
                self.logger.error(f"Failed to download .dvc file from MinIO: {str(e)}")
                return {"status": "error", "message": f"Failed to download .dvc file from MinIO: {str(e)}"}

            # DVC PULL 
            try:
                result = subprocess.run(['dvc', 'pull', str(local_dvc_file_path)], check=True, text=True, capture_output=True, cwd=folder_path)
                self.logger.info(f"Pulled data from remote storage for {dvc_filename}.")
                return {"status": "success", "stdout": result.stdout, "stderr": result.stderr}
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error pulling data from remote storage: {str(e)}")
                return {"status": "error", "stdout": e.stdout, "stderr": e.stderr}

        except FileNotFoundError as e:
            self.logger.error(f"File not found: {str(e)}")
            return {"status": "error", "message": str(e)}
        except Exception as e:
            self.logger.error(f"Error during pull operation: {str(e)}")
            return {"status": "error", "message": str(e)}

   