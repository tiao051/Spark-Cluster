# Project Structure

```
cluster-build/
├── config/                          # Configuration files
│   ├── log4j.properties            # Spark logging configuration
│   └── .env                        # Environment variables
├── scripts/                        # Code scripts
│   ├── python/                     # Python/PySpark scripts
│   │   └── basic_rdd.py           # Example: Basic RDD operations
│   └── scala/                      # Scala scripts for REPL
│       ├── rdd_operations.scala    # Example: RDD operations (fold, min, max)
│       ├── word_count.scala        # Example: Word count and pair RDD
│       └── read_json.scala         # Example: Reading JSON files
├── data/                           # Data files and datasets
│   └── test.json                   # Sample JSON data
├── docs/                           # Documentation
├── docker-compose.yml              # Docker Compose configuration
└── README.md                       # Main README
```

## Directory Descriptions

**config/** - Contains all configuration files needed for Spark and the cluster:
- `log4j.properties` - Controls logging verbosity
- `.env` - Environment variables for the cluster

**scripts/python/** - Python scripts for batch processing:
- Submit these using `spark-submit` from the command line
- Useful for scheduled jobs and data pipelines

**scripts/scala/** - Scala scripts for interactive analysis:
- Load these in the Scala REPL using `:load` command
- Good for exploratory data analysis and testing

**data/** - All data files used by the cluster:
- JSON, CSV, Parquet files, etc.
- Mounted to `/workspace/data` in containers

**docs/** - Additional documentation and guides
