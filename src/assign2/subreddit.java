package assign2;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class subreddit {


    public static void main(String[] arg) {
        String sqlURL = "jdbc:mysql://localhost:3306/reddit";
        String username = "root";
        String password = "root";

        JSONParser parser = new JSONParser();



        String insertIntoSubreddit = "INSERT IGNORE INTO subreddit (id, subreddit)" +
                "VALUES (?,?)";
        String insertIntoLink = "INSERT IGNORE INTO link (id, subreddit_id)" +
                "VALUES (?,?)";
        String insertIntoComment = "INSERT IGNORE INTO subreddit_comment (cmnt_name, id,link_id, parent_id, body," +
                "score, author, created_UTC) VALUES (?,?,?,?,?,?,?,?)";
        try {

            Connection connection = DriverManager.getConnection(sqlURL, username, password);
            connection.setAutoCommit(false);
            PreparedStatement  subredditStatement = connection.prepareStatement(insertIntoSubreddit);
            PreparedStatement linkStatement = connection.prepareStatement(insertIntoLink);
            PreparedStatement commentStatement = connection.prepareStatement(insertIntoComment);
            try(BufferedReader reader = new BufferedReader(new FileReader("RC_2011-07"))) {
                String line = reader.readLine();
                long duration = 0;
                while (line != null) {
                    long heapSize = Runtime.getRuntime().totalMemory()/(1024*1024);

// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
                    //long heapMaxSize = Runtime.getRuntime().maxMemory()/(1024*1024);

                    // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
                    long heapFreeSize = Runtime.getRuntime().freeMemory()/(1024*1024);
                   System.out.println(heapSize);
                    //System.out.println(heapMaxSize);
                    System.out.println(heapFreeSize);
                    System.out.println("===============");
                    if(heapFreeSize < 200){
                        System.out.println("heap is less than 200 and data will be send to mysql");
                        System.out.println("inserting data into mysql.....");
                        long startTime = System.currentTimeMillis();
                        subredditStatement.executeBatch();
                        //subredditStatement.clearBatch();
                        linkStatement.executeBatch();
                        commentStatement.executeBatch();
                        connection.commit();
                        long endTime = System.currentTimeMillis();
                        duration += (endTime-startTime)/60000;
                    }
                    JSONObject jsonObj = (JSONObject) parser.parse(line);
                    String subredditId = (String) jsonObj.get("subreddit_id");
                    //System.out.println(subredditId);
                    String subreddit = (String) jsonObj.get("subreddit");

                    String linkId = (String) jsonObj.get("link_id");

                    String commentName = (String) jsonObj.get("name");
                    String commentId = (String) jsonObj.get("id");
                    String commentBody = (String) jsonObj.get("body");
                    long commentScore = (long) jsonObj.get("score");
                    String commentAuthor = (String) jsonObj.get("author");
                    String commentCreatedUTC = (String) jsonObj.get("created_utc");
                    String commentParentId = (String) jsonObj.get("parent_id");

                    subredditStatement.setString(1,subredditId);
                    subredditStatement.setString(2,subreddit);
                    subredditStatement.addBatch();

                    linkStatement.setString(1, linkId);
                    linkStatement.setString(2, subredditId);
                    linkStatement.addBatch();

                    commentStatement.setString(1, commentName);
                    commentStatement.setString(2, commentId);
                    commentStatement.setString(3, linkId);
                    if(commentParentId.equals(linkId)){
                        System.out.println("parent_id is same with link_id");
                        commentStatement.setString(4, null);
                    }else{
                        commentStatement.setString(4, commentParentId);
                    }


                    commentStatement.setString(5,commentBody);
                    commentStatement.setLong(6, commentScore);
                    commentStatement.setString(7, commentAuthor);
                    commentStatement.setString(8, commentCreatedUTC);

                    commentStatement.addBatch();

                   // System.out.println(line);
                    line = reader.readLine();
                }
                System.out.println("inserting data into mysql..... be patient!!!");
                long startTime = System.currentTimeMillis();
                subredditStatement.executeBatch();
                //subredditStatement.clearBatch();
                linkStatement.executeBatch();
                commentStatement.executeBatch();
                connection.commit();
                long endTime = System.currentTimeMillis();
                duration += (endTime-startTime)/60000;
                System.out.println("Time duration is :" + duration);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }

}
