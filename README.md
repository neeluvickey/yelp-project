# 📊 Yelp Data Pipeline Project

## Overview
This project is designed to build a scalable data pipeline for processing, splitting, uploading, and loading large Yelp datasets into Snowflake using Python.

---

## 📁 Project Structure

```
yelp-project/
│
├── config/
│   └── pipeline_config.cfg        # Contains configuration for staging, AWS, Snowflake, etc.
│
├── staging/
│   ├── json_files/                # Raw Yelp JSON files
│   ├── split_files/               # Auto-generated after splitting JSONs (e.g., business/)
│   └── snowflake_key.pem          # Private key for Snowflake connection
│
├── src/
│   ├── process_pipeline.py        # Main script: orchestrates splitting, uploading, and data loading
│   ├── conn.py                    # Manages Snowflake connection
│   └── utils.py                   # Helper functions (parse_config, split_json_file, upload_to_s3, data_load)
│
├── requirements.txt               # Python package dependencies
└── README.md
```

---

## 🔧 Features

- 🔧 **Configuration-Driven**: All paths and credentials are stored in a config file.
- ✂️ **Split JSON Files**: Large JSONs are split into smaller chunks based on line count.
- ☁️ **Upload to S3**: Maintains folder structure (e.g., `business/`) while uploading split files to AWS S3.
- ❄️ **Load to Snowflake**:
  - Creates an external stage using S3.
  - Auto-creates Snowflake tables (if not exists).
  - Copies data with `STRIP_OUTER_ARRAY = TRUE`.

---

## 🧹 Dataset

Download the original Yelp dataset from the official source:  
[https://business.yelp.com/data/resources/open-dataset/](https://business.yelp.com/data/resources/open-dataset/)

Place the downloaded JSON files into the `staging/json_files/` folder.

---

## Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/neeluvickey/yelp-project.git
   cd yelp-project
   ```
2. Create a virtual environment and activate it:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## 🚀 How to Run

1. Run the pipeline:
   ```bash
   python src/process_pipeline.py
   ```

   This will:
   - Split each JSON in `staging/json_files/`
   - Save to `staging/split_files/<entity>/`
   - Upload to S3
   - Load into Snowflake

---

## 📌 Notes

- Designed to be modular and scalable for future ETL enhancements.
- No hardcoded paths — driven by configuration.
- Optimized for I/O-heavy operations with support for multi-threading (optional enhancement).

---

## 🧑‍💼 Author

**Neelakanteswara ("Neelu")**  
Data Engineer  
*1.9+ years of experience in data pipelines and cloud integrations.*

---

## 📬 Contact

For questions or feedback, feel free to reach out!
