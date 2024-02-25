package it.polimi.ds.map_reduce.utils.database;

import it.polimi.ds.map_reduce.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ServiceUnavailableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class DataManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataManager.class);
    private final Connection dbConnection;

    public DataManager() throws ServiceUnavailableException {
        dbConnection = DBStaticConnectionHandler.getConnection();
    }

    public DataManager(Connection connection) {
        dbConnection = connection;
    }

    @SuppressWarnings("unused") //TODO: remove
    public void saveData(List<Tuple2> data) {
        String query = "UPDATE data SET data_value = ? WHERE data_key = ?";

        PreparedStatement pstatement;
        try {
            pstatement = dbConnection.prepareStatement(query);
            try {
                for (Tuple2 datum : data) {
                    if (datum.value() instanceof String)
                        pstatement.setString(1, datum.value().toString());
                    else
                        pstatement.setInt(1, (Integer) Optional.ofNullable(datum.value()).orElseThrow());
                    pstatement.setInt(2, (Integer) datum.key());
                    pstatement.addBatch();
                }
                List<Integer> results = Arrays.stream(pstatement.executeBatch()).boxed().toList();
            } catch (SQLException e1) {
                LOGGER.trace("[{}] Failure during dava save", this.getClass().getName());
                throw new RuntimeException(e1);
            }
        } catch (SQLException e2) {
            LOGGER.trace("[{}] Couldn't prepare statement in {} method",this.getClass().getName(), new Object() {}.getClass().getEnclosingMethod().getName());
            throw new RuntimeException(e2);
        }
    }


}
