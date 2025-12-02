CREATE TABLE `Your-Project-ID-Here.samples.wikipedia`
(
  title STRING NOT NULL OPTIONS(description="The title of the page, as displayed on the page (not in the URL). Always starts with a capital letter and may begin with a namespace (e.g. \"Talk:\", \"User:\", \"User Talk:\", ... )"),
  id INT64 OPTIONS(description="A unique ID for the article that was revised. These correspond to the order in which articles were created, except for the first several thousand IDs, which are issued in alphabetical order."),
  language STRING NOT NULL OPTIONS(description="Empty in the current dataset."),
  wp_namespace INT64 NOT NULL OPTIONS(description="Wikipedia segments its pages into namespaces (e.g. \"Talk\", \"User\", etc.)\n\nMEDIA = 202; // =-2 in WP XML, but these values must be >0\nSPECIAL = 201; // =-1 in WP XML, but these values must be >0\nMAIN = 0;\nTALK = 1;\nUSER = 2;\nUSER_TALK = 3;\nWIKIPEDIA = 4;\nWIKIPEDIA_TALK = 5;\nIMAGE  = 6;  // Has since been renamed to \"File\" in WP XML.\nIMAGE_TALK = 7;  // Equivalent to \"File talk\".\nMEDIAWIKI = 8;\nMEDIAWIKI_TALK = 9;\nTEMPLATE = 10;\nTEMPLATE_TALK = 11;\nHELP = 12;\nHELP_TALK = 13;\nCATEGORY = 14;\nCATEGORY_TALK = 15;\nPORTAL = 100;\nPORTAL_TALK = 101;\nWIKIPROJECT = 102;\nWIKIPROJECT_TALK = 103;\nREFERENCE = 104;\nREFERENCE_TALK = 105;\nBOOK = 108;\nBOOK_TALK = 109;"),
  is_redirect BOOL OPTIONS(description="Versions later than ca. 200908 may have a redirection marker in the XML."),
  revision_id INT64 OPTIONS(description="These are unique across all revisions to all pages in a particular language and increase with time. Sorting the revisions to a page by revision_id will yield them in chronological order."),
  contributor_ip STRING OPTIONS(description="Typically, either _ip or (_id and _username) will be set. IP information is unavailable for edits from registered accounts. A (very) small fraction of edits have neither _ip or (_id and _username). They show up on Wikipedia as \"(Username or IP removed)\"."),
  contributor_id INT64 OPTIONS(description="Typically, either (_id and _username) or _ip will be set. A (very) small fraction of edits have neither _ip or (_id and _username). They show up on Wikipedia as \"(Username or IP removed)\"."),
  contributor_username STRING OPTIONS(description="Typically, either (_id and _username) or _ip will be set. A (very) small fraction of edits have neither _ip or (_id and _username). They show up on Wikipedia as \"(Username or IP removed)\"."),
  timestamp INT64 NOT NULL OPTIONS(description="In Unix time, seconds since epoch."),
  is_minor BOOL OPTIONS(description="Corresponds to the \"Minor Edit\" checkbox on Wikipedia's edit page."),
  is_bot BOOL OPTIONS(description="A special flag that some of Wikipedia's more active bots voluntarily set."),
  reversion_id INT64 OPTIONS(description="If this edit is a reversion to a previous edit, this field records the revision_id that was reverted to. If the same article text occurred multiple times, then this will point to the earliest revision. Only revisions with greater than fifty characters are considered for this field. This is to avoid labeling multiple blankings as reversions."),
  comment STRING OPTIONS(description="Optional user-supplied description of the edit. Section edits are, by default, prefixed with \"/* Section Name */ \"."),
  num_characters INT64 NOT NULL OPTIONS(description="The length of the article after the revision was applied.")
)
OPTIONS(
  labels=[("dataplex-dp-published-project", "daui-storage"), ("dataplex-dp-published-location", "us-central1"), ("dataplex-dp-published-scan", "af83fd049-871e-486e-86bb-406eb82a794d")]
);
