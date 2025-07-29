import os 
import json
import sys
import pandas as pd
from promptflow.client import load_flow
from tqdm import tqdm



class LLMasJudgeExecMatch:
    """
    A class that uses an LLM-based flow to evaluate whether a predicted SQL query result
    matches the gold (reference) result given a natural language question.

    The evaluation logic is defined in a `llm_as_judge_exec_match.prompty` file.
    """

    def __init__(self, model_config):
        """
        Initializes the LLMasJudgeExecMatch instance.

        Args:
            model_config (dict): Model configuration used for the prompt flow.
        """
        # Absolute path to the `.prompty` file defining the flow
        prompty_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "prompts/llm_as_judge_exec_match.prompty")
        )

        # Load the LLM prompt flow with the given model configuration
        self._flow = load_flow(
            source=prompty_path,
            model={"configuration": model_config, "parameters": {}}
        )

    def __call__(self, *, question: str, gold_result: str, pred_result: str, **kwargs):
        """
        Invokes the prompt flow to compare gold and predicted query results.

        Args:
            question (str): The original natural language question.
            gold_result (str): The ground truth result (reference execution result).
            pred_result (str): The model-generated result (prediction execution result).

        Returns:
            dict or str: The parsed response from the LLM if JSON-deserializable,
                         otherwise the raw response string.
        """
        # Run the prompt flow with inputs
        llm_response = self._flow(
            question=question,
            gold_result=gold_result,
            pred_result=pred_result
        )

        # Try to parse the response as JSON, return raw string if parsing fails
        try:
            response = json.loads(llm_response)
        except Exception as ex:
            response = llm_response

        return response
