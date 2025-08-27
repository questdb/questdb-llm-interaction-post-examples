# QuestDB LLM interaction post examples

Examples of LLM integration with QuestDB for the blog post at https://questdb.com/blog/questdb-and-llm-interaction/

# Post intro

> Thanks to Large Language Models (LLMs), interacting with your data is easier than ever. Previously, you needed to know SQL, a coding language like Python, or some Business Intelligence (BI) tool to query and analyze your data stored away in some database. But now, you can simply ask questions in plain English, and let the LLM contextualize that data for you.
>
> Every day, new advancements in AI are lowering this barrier further. At first, one would need to ask an LLM to write out a SQL query to translate prompts like "show me the average price of Bitcoin in the last five days" and copy-paste that into a dashboard or BI tool. Then came Model Context Protocols (MCPs), which standardized how LLMs can retrieve information from external sources that the model was not trained on. Once the MCP server is connected, you can now interact with the database directly from your LLM interface (whether that be your terminal, IDE, or LLM UI) instead of having to switch context. And now, we are seeing semi-autonomous agents automating more and more of this away.
>
> In this article, we'll explore how to leverage LLM tools like Claude Code and Goose to interact with QuestDB. We'll first look at utilizing QuestDB's built-in REST API as well as using an OSS PostgreSQL MCP server. Let's explore how these two approaches differ in philosophy and how we can leverage either path based on the use case.

Full text at  https://questdb.com/blog/questdb-and-llm-interaction/
