package com.group21.server.processor;

import java.util.Date;

import com.group21.server.queries.deleteQuery.DeleteQueryExecutor;
import com.group21.server.queries.insert.InsertQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.QueryType;
import com.group21.server.queries.createtable.CreateTableQueryExecutor;

public class QueryProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcessor.class);

    private QueryProcessor() {
    }

    public static void process(String query) {
        query = query.toUpperCase();

        long startTime = System.currentTimeMillis();
        LOGGER.debug("Execution started for query - '{}' on {}", query, new Date(startTime));
        QueryType queryType = QueryType.from(query);

        switch (queryType) {
            case CREATE:
                CreateTableQueryExecutor executor = new CreateTableQueryExecutor();
                executor.execute(query);
                break;
            case DELETE:
                DeleteQueryExecutor deleteQueryExecutor = new DeleteQueryExecutor();
                deleteQueryExecutor.execute(query);
                break;
            case INSERT:
                InsertQueryExecutor insertQueryExecutor = new InsertQueryExecutor();
                insertQueryExecutor.execute(query);
                break;
            case DROP:
                break;
            default:
                LOGGER.info("Provided query is not yet supported by this tool.");
                break;
        }

        long endTime = System.currentTimeMillis();
        LOGGER.debug("Execution completed for query - '{}' on {}", query, new Date(endTime));
        LOGGER.debug("Total execution time for query - '{}' is {}ms.", query, (endTime - startTime));
        LOGGER.info("Query executed in {}ms.", (endTime - startTime));
    }
}
