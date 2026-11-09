-- Saved Functions are Private Tools (ADR 0033). Step 65 renamed the schema
-- and the values its checks name; this step rewrites the other stored values
-- that name Saved Functions, so retained state reads the way new state does.
--
-- Stored JSON is compact, so each JSON pattern starts at a key or at a
-- string's opening quote. Text a Program or an upstream controls, such as an
-- error message or a Final Value, stays as it was written. The Call Paths and
-- sources that Runs recorded stay as well.

-- A Coral failure code names a Private Tool. An Invocation's Coral failure
-- message is Coral's own.
UPDATE tool_invocations
SET failure_code = 'private_tool_' || SUBSTR(failure_code, 16),
    failure_message = CASE failure_message
        WHEN 'Saved Function was not found' THEN 'Private Tool was not found'
        ELSE failure_message
    END
WHERE SUBSTR(failure_code, 1, 15) = 'saved_function_';

UPDATE program_run_records
SET failure_code = 'private_tool_' || SUBSTR(failure_code, 16)
WHERE SUBSTR(failure_code, 1, 15) = 'saved_function_';

-- A failed Run keeps the Program's error. A Coral failure in it names a
-- Private Tool, and each stack frame in a Private Tool's source names that
-- source `Private Tool <Call Path>`. A message that is exactly Coral's is
-- renamed even when a Program threw it; any other Program message stays.
UPDATE program_run_records
SET failure_message = CASE failure_message
        WHEN 'ToolError: Saved Function was not found' THEN 'ToolError: Private Tool was not found'
        ELSE failure_message
    END,
    failure_error_json = REPLACE(REPLACE(REPLACE(failure_error_json,
        '"code":"saved_function_not_found"', '"code":"private_tool_not_found"'),
        '"message":"Saved Function was not found"', '"message":"Private Tool was not found"'),
        '"source":"Saved Function coral.private.', '"source":"Private Tool coral.private.')
WHERE failure_error_json LIKE '%Saved Function%'
    OR failure_error_json LIKE '%saved_function%';

-- A Task keeps the search and describe responses Coral returned, the Private
-- Tool a save or delete named, and the failure of an attempt Coral rejected.
-- Those now name the kind and retriever `private_tool`, and each failure code
-- names a Private Tool.
UPDATE task_interactions
SET evidence_json =
    REPLACE(REPLACE(REPLACE(REPLACE(evidence_json,
        '"kind":"saved_function"', '"kind":"private_tool"'),
        '"retriever":"saved_function"', '"retriever":"private_tool"'),
        '"code":"saved_function_', '"code":"private_tool_'),
        '"code":"invalid_saved_function', '"code":"invalid_private_tool')
WHERE evidence_json LIKE '%saved_function%';
