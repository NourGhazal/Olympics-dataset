import os
import shutil

from airflow.utils.helpers import parse_template_string
from airflow.utils.log.file_task_handler import FileTaskHandler


class FileTaskRotationHandler(FileTaskHandler):

    def __init__(self, base_log_folder, filename_template, folder_task_template, retention):
        """
        :param base_log_folder: Base log folder to place logs.
        :param filename_template: template filename string.
        :param folder_task_template: template folder task path.
        :param retention: Number of folder logs to keep
        """
        super(FileTaskRotationHandler, self).__init__(base_log_folder, filename_template)
        self.retention = retention
        self.folder_task_template, self.folder_task_template_jinja_template = \
            parse_template_string(folder_task_template)

    @staticmethod
    def _get_directories(path='.'):
        return next(os.walk(path))[1]