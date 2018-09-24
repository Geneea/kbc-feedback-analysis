Input:
* `id` - ID of the user comment
* `text` - main text of the user-feedback
* `positives` - section with positive comments
* `negatives` - section with negative comments

Input options:
* `language` - the language of the text; leave empty for automatic detection
* `domain` - the domain or type of the text
* `feedback_entities` - entity types which should be analyzed for sentiment
* `feedback_relations` - relation types which should be analyzed for sentiment
* `correction` - indicates whether common typos should be corrected before analysis
* `diacritization` - before analysing Czech text where diacritics are missing, add all the wedges and accents. For example, _Muj ctyrnohy pritel_ is changed to _Můj čtyřnohý přítel_.
* `use_beta` - use Geneea's beta server (use only when instructed to do so)
* `advanced` - additional parameters as a JSON object (use only when instructed to do so)


The result contains five tables:

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

  There are multiple rows per one comment. All `id` columns plus the `index` column are part of the primary key.

* `analysis-result-entities.csv` with entity-level results has the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `type` type of the found entity, e.g. _person_, _organization_ or _tag_, (primary key)
    * `text` disambiguated and standardized form of the entity, e.g. _John Smith_, _Keboola_, _safe carseat_, (primary key)
    * `score` relevance score of the entity, e.g. _0.8_
    * `entityUid` unique ID of the entity, may be empty
    * `sentimentValue` detected sentiment of the entity, from an interval _\[-1.0; 1.0\]_
    * `sentimentPolarity` detected sentiment of the entity (_-1_, _0_ or _1_)
    * `sentimentLabel` sentiment of the entity as a label (_negative_, _neutral_ or _positive_)

  There are multiple rows per one comment. All `id` columns plus the `type` and `text` columns are part of the primary key.

  Note that the table also contains topic tags, marked as _tag_ in the `type` column.

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

  There are multiple rows per one comment. All `id` columns plus the `type`, `name`, `subject` and `object` columns are part of the primary key.

* `analysis-result-full.csv` with full analysis results in the following columns:
    * all `id` columns from the input table (used as primary keys)
    * `binaryData` serialized data with full analysis as Base64

  This table can be used as an input for the **Geneea Frida** writer app. 
