package cn.ktte.log;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

import java.util.List;

/**
 * 日志解析类测试
 */
public class TestLog {
    public static void main(String[] args) throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException, DissectionFailure {

        //接下来的案例,听听就行了.主要是为了便于后面的理解.

        //定义格式化字符串
        String logformat = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\" \"%{Addr}i\"";
        String logline = "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\" \"jquery-ui-theme=Eggplant; BuI=SomeThing; Apache=127.0.0.1.1351111543699529\" \"beijingshi\"";
        Parser<Object> dummyParser= new HttpdLoglineParser<>(Object.class, logformat);

        List<String> possiblePaths;
        possiblePaths = dummyParser.getPossiblePaths();

        // If you want to call 'getCasts' then the actual parser needs to be constructed.
        // Simply calling getPossiblePaths does not build the actual parser.
        // Because we want this for all possibilities yet we are never actually going to use this instance of the parser
        // We simply give it a random method with the right signature and tell it we want all possible paths
        dummyParser.addParseTarget(String.class.getMethod("indexOf", String.class), possiblePaths);

        for (String path : possiblePaths) {
            System.out.println("path: " + path + "       casts:    " + dummyParser.getCasts(path));
        }
        //=========================================================================================



        // 获取一个LogParser的解析对象.
        // 参数1: 我接下来要将日志解析为什么对象.
        // 参数2: 我应该按照什么格式进行解析.
        Parser<MyRecord> parser = new HttpdLoglineParser<>(MyRecord.class, logformat);
//        MyRecord record = new MyRecord();
        // 使用解析对象,开始进行解析
        // 参数: 我们要解析的原始数据
        MyRecord result = parser.parse(logline);

        System.out.println(result);

        System.out.println("-------------------------------------------------------------------");

        HttpdLoglineParser<MyClickLog> myParser = new HttpdLoglineParser<>(MyClickLog.class, logformat);
        //我们在进行数据解析之前,需要指定一下解析器,就是告诉HttpdLoglineParser它,应该如何去进行解析.
        myParser.addParseTarget("setUserClientIp", "IP:connection.client.host");
        MyClickLog myClickLog = myParser.parse(logline);
        System.out.println(myClickLog);


    }
}
