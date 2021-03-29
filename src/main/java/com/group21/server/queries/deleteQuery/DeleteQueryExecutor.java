package com.group21.server.queries.deleteQuery;

public class DeleteQueryExecutor {

    private DeleteParser deleteParser;

    public DeleteQueryExecutor(){
        this.deleteParser = new DeleteParser();
    }

    public void execute(String query)  {
        deleteParser.isValid(query);

    }
}
