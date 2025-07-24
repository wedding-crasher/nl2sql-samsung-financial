import os 
import json
import sys
import pandas as pd
from dotenv import load_dotenv
from azure.ai.evaluation import AzureOpenAIModelConfiguration
from promptflow.client import load_flow
from tqdm import tqdm

model_config = AzureOpenAIModelConfiguration(
    azure_endpoint= os.environ["AZURE_ENDPOINT"],
    azure_key = os.environ["AZURE_API_KEY"],
    azure_deployment = os.environ["AZURE_4O_DEPLOYMENT"],
    api_version = os.environ["AZURE_4O_API_VERSION"]
)

class LLMasJudgeRawSQL:
    def __init__(self, model_config):
        current_dir = os.getcwd()
        prompty_path = os.path.join(current_dir, "prompts/llm-as-judge-rawsql.prompty")
        # 1 Under bar represents don't directly access in the outside
        self._flow = load_flow(source=prompty_path,model = {"configuration": model_config, "parameters": {}})

    
    def __call__(self,*, question: str, gold_sql:str, pred_sql: str, **kwargs):
        llm_response = self._flow(question = question, gold_sql = gold_sql, pred_sql = pred_sql)
        try:
            response = json.load(llm_response)
        except Exception as ex:
            response = llm_response
        return response