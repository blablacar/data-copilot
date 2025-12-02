CREATE TABLE `Your-Project-ID-Here.samples.shakespeare`
(
  word STRING NOT NULL OPTIONS(description="A single unique word (where whitespace is the delimiter) extracted from a corpus."),
  word_count INT64 NOT NULL OPTIONS(description="The number of times this word appears in this corpus."),
  corpus STRING NOT NULL OPTIONS(description="The work from which this word was extracted."),
  corpus_date INT64 NOT NULL OPTIONS(description="The year in which this corpus was published.")
);
