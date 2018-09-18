# User feedback analysis in KBC using Geneea NLP platform

Integration of the [Geneea API](https://api.geneea.com) with [Keboola Connection](https://connection.keboola.com).

This is a Docker container used for running user-feedback NLP analysis jobs in the KBC.
Automatically built Docker images are available at [Docker Hub Registry](https://hub.docker.com/r/geneea/kbc-feedback-analysis/).

## Building a container
To build this container manually one can use:

```
git clone https://github.com/Geneea/kbc-feedback-analysis.git
cd kbc-feedback-analysis
sudo docker build --no-cache -t geneea/kbc-feedback-analysis .
```

## Running a container
This container can be run from the Registry using:

```
sudo docker run \
--volume=/home/ec2-user/data:/data \
--rm \
geneea/kbc-feedback-analysis:latest
```
Note: `--volume` needs to be adjusted accordingly.

## Sample configuration
Mapped to `/data/config.json`

```
{
  "storage": {
    "input": {
      "tables": [
        {
          "destination": "comments.csv"
        }
      ]
    }
  },
  "parameters": {
    "user_key": "<ENTER API KEY HERE>",
    "columns": {
      "id": ["feedback_id"],
      "text": ["summary"],
      "positives": ["pos_1", "pos_2"],
      "negatives": ["neg_1", "neg_2"]
    },
    "language": "cs",
    "domain": "retail",
    "feedback_entities": ["service", "product"],
    "correction": "aggresive",
    "diacritization": "auto",
    "use_beta": false
  }
}
```

## Output format

The results of the NLP analysis are written into five tables.

* `analysis-result-comments.csv` with comment-level results in the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `language` detected language of the comment, as ISO 639-1 language code
    * `sentimentValue` detected sentiment of the comment, from an interval _\[-1.0; 1.0\]_
    * `sentimentPolarity` detected sentiment of the comment (_-1_, _0_ or _1_)
    * `sentimentLabel` sentiment of the comment as a label (_negative_, _neutral_ or _positive_)
    * `usedChars` the number of characters used by this comment

* `analysis-result-sentences.csv` with sentence-level results has the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `index` zero-based index of the sentence in the comment, (primary key)
    * `segment` text segment where the sentence is located
    * `text` the sentence text
    * `sentimentValue` detected sentiment of the sentence, from an interval _\[-1.0; 1.0\]_
    * `sentimentPolarity` detected sentiment of the sentence (_-1_, _0_ or _1_)
    * `sentimentLabel` sentiment of the sentence as a label (_negative_, _neutral_ or _positive_)

* `analysis-result-entities.csv` with entity-level results has the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `type` type of the found entity, e.g. _person_, _organization_ or _tag_, (primary key)
    * `text` disambiguated and standardized form of the entity, e.g. _John Smith_, _Keboola_, _safe carseat_, (primary key)
    * `score` relevance score of the entity, e.g. _0.8_
    * `entityUid` unique ID of the entity, may be empty
    * `sentimentValue` detected sentiment of the entity, from an interval _\[-1.0; 1.0\]_
    * `sentimentPolarity` detected sentiment of the entity (_-1_, _0_ or _1_)
    * `sentimentLabel` sentiment of the entity as a label (_negative_, _neutral_ or _positive_)

* `analysis-result-relations.csv` with relations-level results has the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `type` type of the found relation, _VERB_ or _ATTR_, (primary key)
    * `name` textual name of the relation, e.g. _buy_ or _smart_, (primary key)
    * `subject` possible subject of the relation (primary key)
    * `object` possible object of the relation (primary key)
    * `subjectType` type of the relation's subject
    * `objectType` type of the relation's object
    * `subjectUid` unique ID of the relation's subject
    * `objectUid` unique ID of the relation's object
    * `sentimentValue` detected sentiment of the relation, from an interval _\[-1.0; 1.0\]_
    * `sentimentPolarity` detected sentiment of the relation (_-1_, _0_ or _1_)
    * `sentimentLabel` sentiment of the relation as a label (_negative_, _neutral_ or _positive_)

* `analysis-result-full.csv` with full analysis results in the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `binaryData` serialized data with full analysis as Base64

  This table can be used as an input for the **Geneea Frida** writer app. 
