## Setup and Run

### 1. Clone the repository

### 2. Create virtual environment

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download dataset

Run:

```bash
python data/load_data.py
```

### 5. Preprocess data

```bash
python spark/preprocess.py
```

### 6. Train models

```bash
python ml/train.py
```

### 7. Run API In One Terminal

```bash
uvicorn api.main:app --reload
```

### 8. Run dashboard simultaneously in another terminal

```bash
streamlit run dashboard/app.py
```

### 9. Use the system

* Enter a prompt in the dashboard
