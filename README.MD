# 📊 Multi-Angle NL2SQL Evaluation Pipeline

**Robust and comprehensive evaluation is the key to building better NL2SQL models.**  
This repository introduces a multi-angle evaluation framework that goes beyond simple execution match and offers LLM-based, rule-based, and error-focused evaluation pipelines to reveal both model and evaluation weaknesses.

---

## 🚀 Overview

This project investigates **state-of-the-art NL2SQL evaluation methodologies** and builds a **robust evaluation pipeline**.  
By combining LLM-based evaluation with traditional rule-based approaches, we not only assess model performance but also **evaluate the evaluators themselves**.

You will learn:
- How to evaluate NL2SQL models using **LLMs as judges**
- How to compare SQL outputs via **execution results**, **query structure**, and **system resource efficiency**
- How to identify **LLM’s limitations** in understanding SQL and certain data types

---

## 🧪 Evaluation Methods

### 🧠 Method 1: LLM-Based Evaluation
- **LLM-as-a-Judge: Raw SQL Comparison**  
  Compare the predicted SQL with the gold SQL directly using an LLM to judge correctness.

- **LLM-as-a-Judge: Execution Match**  
  Let the LLM judge whether the predicted SQL result matches the expected output.

### ⚙️ Method 2: Rule-Based Evaluation
- **Execution Match**  
  Compares the execution results of predicted vs. gold SQL using database queries.

- **Valid Efficiency Score (VES)**  
  Evaluates how efficiently the predicted SQL runs in terms of runtime and memory compared to the gold SQL.

---

## 🧭 Evaluating the Evaluators

We go a step further and **test the evaluation pipelines themselves**:
- How accurately can they detect SQL errors?
- What types of data are LLMs particularly weak at evaluating?
- How consistent are LLM judgments across similar queries?

---

## 📂 Main Files

- `evaluation/llm_evaluators_evaluation.ipynb`  
  → Learn how to use LLMs as judges for NL2SQL evaluation  
  → Analyze where LLM-based evaluation is strong or weak

- `evaluation/rule_based_evaluators_evaluation.ipynb`  
  → Evaluate SQL outputs using traditional rule-based logic  
  → Includes performance, runtime, and execution match evaluation

---

## ⚒️ Prerequisites

1. **Dataset Setup**  
   Download and configure the dataset as instructed in `dataset/README.md`.

2. **Azure OpenAI Setup**  
   LLM-based evaluation requires deployment of your own Azure OpenAI endpoint.  
   Refer to `.env.sample` for required configuration.

---

## 📦 Dependency: NL2SQL360 (Customized)

We include a customized version of the [NL2SQL360](https://github.com/HKUSTDial/NL2SQL360) library in `/nl2sql360` for rule-based evaluation.  
Thanks to the amazing work by DialLab at HKUST for the original implementation.

---

## 🤝 Acknowledgements

- **DialLab @ HKUST** – For their NL2SQL360 framework
- **OpenAI & Azure** – For enabling powerful LLM-based evaluation

---

## 📍 Why This Matters

NL2SQL models are only as good as how we evaluate them.  
This repository offers a way to:
- Benchmark models fairly across multiple dimensions
- Detect subtle errors that escape traditional evaluation
- Understand **what LLMs still struggle with in SQL understanding**

If you're building NL2SQL models, evaluating them, or analyzing model behavior, this repository is for you.

---

BibTeX:
```bibtex
@article{nl2sql360,
  author       = {Boyan Li and
                  Yuyu Luo and
                  Chengliang Chai and
                  Guoliang Li and
                  Nan Tang},
  title        = {The Dawn of Natural Language to {SQL:} Are We Fully Ready? },
  journal      = {Proc. {VLDB} Endow.},
  volume       = {17},
  number       = {11},
  pages        = {3318--3331},
  year         = {2024}
}
```