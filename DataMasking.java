/** Data masking for a field called ACCOUNTID
*This Java code does the following:
1. Connects to the database.
2. Reads all distinct ACCOUNTIDs.
3. Generates a scrambled version for each.
4. Stores the mapping.
5. Updates each table using the mapping.
*/

import java.sql.*;
import java.util.*;

public class AccountIdScrambler {

    private static final String DB_URL = "jdbc:yourdb://localhost:5432/yourdb";
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "password";

    // List of tables and the column that references ACCOUNTID
    private static final Map<String, String> TABLES = Map.of(
        "Accounts", "ACCOUNTID",
        "Orders", "ACCOUNTID",
        "Transactions", "ACCOUNTID"
    );

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            conn.setAutoCommit(false);

            // 1. Get distinct ACCOUNTIDs
            Set<String> accountIds = getAllAccountIds(conn);

            // 2. Generate mapping
            Map<String, String> idMapping = generateScrambledMapping(accountIds);

            // 3. Store mapping in temp table
            createMappingTable(conn);
            insertMapping(conn, idMapping);

            // 4. Update all referencing tables
            for (String table : TABLES.keySet()) {
                updateTable(conn, table, TABLES.get(table));
            }

            conn.commit();
            System.out.println("Scrambling complete. Referential integrity preserved.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Set<String> getAllAccountIds(Connection conn) throws SQLException {
        Set<String> ids = new HashSet<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT DISTINCT ACCOUNTID FROM Accounts")) {
            while (rs.next()) {
                ids.add(rs.getString("ACCOUNTID"));
            }
        }
        return ids;
    }

    private static Map<String, String> generateScrambledMapping(Set<String> originalIds) {
        Map<String, String> mapping = new HashMap<>();
        Random rand = new Random();

        for (String original : originalIds) {
            String scrambled;
            do {
                scrambled = "ACC" + (100000 + rand.nextInt(900000)); // Example format
            } while (mapping.containsValue(scrambled));
            mapping.put(original, scrambled);
        }
        return mapping;
    }

    private static void createMappingTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS ACCOUNTID_MAPPING");
            stmt.executeUpdate("""
                CREATE TABLE ACCOUNTID_MAPPING (
                    OLD_ID VARCHAR(255) PRIMARY KEY,
                    NEW_ID VARCHAR(255) UNIQUE
                )
            """);
        }
    }

    private static void insertMapping(Connection conn, Map<String, String> mapping) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ACCOUNTID_MAPPING (OLD_ID, NEW_ID) VALUES (?, ?)")) {
            for (Map.Entry<String, String> entry : mapping.entrySet()) {
                ps.setString(1, entry.getKey());
                ps.setString(2, entry.getValue());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void updateTable(Connection conn, String table, String column) throws SQLException {
        String sql = String.format("""
            UPDATE %s
            SET %s = (
                SELECT NEW_ID FROM ACCOUNT
