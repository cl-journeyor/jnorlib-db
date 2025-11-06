package jnorlib_db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Objects;
import javax.sql.DataSource;

public class JdbcManager implements AutoCloseable {
    private final DataSource source;
    private Connection connection;
    private Statement statement;
    private ArrayList<PreparedStatement> preparedStatements = new ArrayList<>();
    private ArrayList<ResultSet> resultSets = new ArrayList<>();

    public JdbcManager(DataSource source) {
        this.source = Objects.requireNonNull(source);
    }

    private void ensureConnection() throws SQLException {
        if (connection == null) {
            connection = source.getConnection();
        }
    }

    private void ensureConnectionAndStatement() throws SQLException {
        ensureConnection();

        if (statement == null) {
            statement = connection.createStatement();
        }
    }

    public void execute(String sql) throws SQLException {
        Objects.requireNonNull(sql);

        ensureConnectionAndStatement();

        statement.execute(sql);
    }

    public void execute(PreparedStatement preparedStatement) throws SQLException {
        Objects.requireNonNull(preparedStatement);

        preparedStatement.execute();
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        Objects.requireNonNull(sql);

        ensureConnectionAndStatement();

        ResultSet resultSet = statement.executeQuery(sql);

        resultSets.add(resultSet);

        return resultSet;
    }

    public ResultSet executeQuery(PreparedStatement preparedStatement) throws SQLException {
        Objects.requireNonNull(preparedStatement);

        ResultSet resultSet = preparedStatement.executeQuery();

        resultSets.add(resultSet);

        return resultSet;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        Objects.requireNonNull(sql);

        ensureConnection();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        preparedStatements.add(preparedStatement);

        return preparedStatement;
    }

    @Override
    public void close() throws SQLException {
        for (ResultSet resultSet : resultSets) {
            resultSet.close();
        }
        for (PreparedStatement preparedStatement : preparedStatements) {
            preparedStatement.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
