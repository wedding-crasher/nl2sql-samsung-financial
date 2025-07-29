import os 
import json
import sys
import pandas as pd
from promptflow.client import load_flow
from tqdm import tqdm



class LLMasJudgeRawSQL:
    """
    A class that uses an LLM-based prompt flow to judge whether a predicted SQL query
    is semantically correct compared to a gold (reference) SQL query, given a question.

    The evaluation logic is defined in a `llm_as_judge_raw_sql.prompty` file.
    """

    def __init__(self, model_config):
        """
        Initializes the LLMasJudgeRawSQL instance.

        Args:
            model_config (dict): Model configuration used to load the prompt flow.
        """
        # Construct absolute path to the .prompty file defining the evaluation prompt
        prompty_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "prompts/llm_as_judge_raw_sql.prompty")
        )

        # Load the prompt flow with the specified model and parameters
        self._flow = load_flow(
            source=prompty_path,
            model={"configuration": model_config, "parameters": {}}
        )

    def __call__(self, *, question: str, gold_sql: str, pred_sql: str, **kwargs):
        """
        Calls the prompt flow with the input question and SQL pairs.

        Args:
            question (str): The original natural language question.
            gold_sql (str): The reference (correct) SQL query.
            pred_sql (str): The model-generated SQL query to be evaluated.

        Returns:
            dict or str: The parsed JSON result from the LLM judgment,
                         or raw response string if JSON parsing fails.
        """
        # Execute the prompt flow with provided inputs
        llm_response = self._flow(
            question=question,
            gold_sql=gold_sql,
            pred_sql=pred_sql
        )

        # Attempt to parse the response as JSON
        try:
            response = json.loads(llm_response)
        except Exception as ex:
            response = llm_response

        return response
