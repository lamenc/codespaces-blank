
import os
import sys
import shutil
from pathlib import Path
import asyncio
import logging

logger = logging.getLogger(__name__)

class OSController:
    @staticmethod
    async def launch_app(app_name: str):
        """
        Launches a native application or service.
        """
        command = ""
        if sys.platform == "win32":
            command = f"start {app_name}"
        elif sys.platform == "darwin":
            command = f"open -a {app_name}"
        else:
            command = f"{app_name} &"
        
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error(f"Error launching {app_name}: {stderr.decode().strip()}")
            else:
                logger.info(f"Successfully launched {app_name}: {stdout.decode().strip()}")
        except Exception as e:
            logger.error(f"Exception launching {app_name}: {e}")

    @staticmethod
    async def delete_file(filename: str):
        """
        Permanently removes a file using pathlib.Path.unlink().
        """
        file_path = Path(filename)
        try:
            file_path.unlink()
            logger.info(f"Successfully deleted file: {filename}")
        except FileNotFoundError:
            logger.error(f"File not found: {filename}")
        except Exception as e:
            logger.error(f"Error deleting file {filename}: {e}")

    @staticmethod
    async def copy_file(source: str, destination: str):
        """
        Uses shutil.copy2 to duplicate a file.
        """
        try:
            shutil.copy2(source, destination)
            logger.info(f"Successfully copied {source} to {destination}")
        except FileNotFoundError:
            logger.error(f"Source file not found: {source}")
        except Exception as e:
            logger.error(f"Error copying file from {source} to {destination}: {e}")

