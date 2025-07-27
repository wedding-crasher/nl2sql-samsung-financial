import os 
import json
import sys
import pandas as pd
from promptflow.client import load_flow
from tqdm import tqdm



class LLMasJudgeRawSQL:
    def __init__(self, model_config):
        prompty_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "prompts/llm_as_judge_raw_sql.prompty"))
        # 1 Under bar represents don't directly access in the outside
        self._flow = load_flow(source=prompty_path,model = {"configuration": model_config, "parameters": {}})

    
    def __call__(self,*, question: str, gold_sql:str, pred_sql: str, **kwargs):
        llm_response = self._flow(question = question, gold_sql = gold_sql, pred_sql = pred_sql)
        try:
            response = json.load(llm_response)
        except Exception as ex:
            response = llm_response
        return response