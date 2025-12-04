# BlaBlaCar Data Copilot

Build data analysis using BigQuery INFORMATION_SCHEMA and GitHub Copilot premium models.

<img src="assets/images/blablacar_data_copilot.png" alt="Data Copilot Diagram" width="400"/>

## Get Started

### Configure environment file
Copy `.env.example` to `.env` in the root directory and configure your BigQuery project:

```console
cp .env.example .env
```

Edit the `.env` file and set your configuration:

```properties
# Your billing project ID
PROJECT_ID=Your-Project-ID-Here

# BigQuery location for query execution
BQ_REGION=EU

# Optional: Regex patterns for filtering datasets and tables
DATASET_FILTER_REGEX="^(samples|covid19_google_mobility|covid19_geotab_mobility_impact)$"
TABLE_FILTER_REGEX=
```

### Initialize Sample Data (Optional)

Copy sample datasets from `bigquery-public-data` to your project. This will generate data copy cost.

```shell
make init-samples
```

### Update Data Warehouse Table Structure

```shell
make pull-queries
```

### Start Copilot

To start using GitHub Copilot for generating new analyses,
just open Copilot in your IDE, select agent mode and *Claude Sonnet 4.5*.

To ease the process, you can add folders of specific tables or notes as context.
You can also mention past analyses that might be related.

Then just ask the question you want to answer in natural language and Copilot will help you build the analysis step by step.

Enjoy!

#### Example of prompts to get started

- "What was the impact of covid on airport and port traffic ?"
- "What was the impact of covid on commercial traffic ?"
- "Can you describe the contribution pattern on wikipedia ?"
