import org.neo4j.driver.v1.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by zuston on 17/4/4.
 */
public class neo4jToFile {
//    public static final  Logger log = LoggerFactory.getLogger(neo4jToFile.class);

    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "shacha" ) );
        Session session = driver.session();

        StatementResult result = session.run( "Match (n:Page) return n");
        ArrayList<String> list = new ArrayList<String>();
        while ( result.hasNext() )
        {
            Record record = result.next();
            list.add(record.get(0).get("title").asString());
        }
        System.out.println("节点寻找完毕");
        System.out.println();
//        log.debug("节点寻找完毕");
        String path = "./t.txt";
        File file=new File(path);
        if(!file.exists())
            file.createNewFile();

        FileOutputStream out=new FileOutputStream(file,false);

        for (String str:list){
//            System.out.println("节点："+str+" 关系");
//            log.debug("节点："+str+" 关系");
            result = session.run("Match (n:Page)-[: Link]->(end:Page) where n.title={name}  return end",parameters( "name", str ));
            StringBuffer sb = new StringBuffer();
            sb.append(str+"++");
            while (result.hasNext()){
                Record record = result.next();
                sb.append(record.get(0).get("title").asString());
                sb.append("--");
            }
            String s = sb.substring(0,sb.length()-2);
            s+="\n";
            out.write(s.getBytes("utf-8"));
        }
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime);
        out.close();
        session.close();
        driver.close();
    }
}
