from sensor.entity import artifact_entity, config_entity
from sensor.exception import SensorException
from sensor.logger import logging
from typing import Optional
import os, sys


class DataTransformation:
    def initiate_data_transformation(
        self,
    ) -> artifact_entity.DataTransformationArtifact:
        try:
            pass
        except Exception as e:
            raise SensorException(e, sys)
