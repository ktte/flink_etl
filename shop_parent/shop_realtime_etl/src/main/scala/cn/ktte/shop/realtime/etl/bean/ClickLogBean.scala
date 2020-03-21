package cn.ktte.shop.realtime.etl.bean

import com.alibaba.fastjson.JSON
import nl.basjes.parse.httpdlog.HttpdLoglineParser

import scala.beans.BeanProperty

class ClickLogBean {

  //用户id信息
  private[this] var _connectionClientUser: String = _
  def setConnectionClientUser (value: String): Unit = { _connectionClientUser = value }
  def getConnectionClientUser = { _connectionClientUser }

  //ip地址
  private[this] var _ip: String = _
  def setIp (value: String): Unit = { _ip = value }
  def getIp = {  _ip }

  //请求时间
  private[this] var _requestTime: String = _
  def setRequestTime (value: String): Unit = { _requestTime = value }
  def getRequestTime = { _requestTime }

  //请求方式
  private[this] var _method:String = _
  def setMethod(value:String) = {_method = value}
  def getMethod = {_method}

  //请求资源
  private[this] var _resolution:String = _
  def setResolution(value:String) = { _resolution = value}
  def getResolution = { _resolution }

  //请求协议
  private[this] var _requestProtocol: String = _
  def setRequestProtocol (value: String): Unit = { _requestProtocol = value }
  def getRequestProtocol = { _requestProtocol }

  //响应码
  private[this] var _responseStatus: Int = _
  def setResponseStatus (value: Int): Unit = { _responseStatus = value }
  def getResponseStatus = { _responseStatus }

  //返回的数据流量
  private[this] var _responseBodyBytes: String = _
  def setResponseBodyBytes (value: String): Unit = { _responseBodyBytes = value }
  def getResponseBodyBytes = { _responseBodyBytes }

  //访客的来源url
  private[this] var _referer: String = _
  def setReferer (value: String): Unit = { _referer = value }
  def getReferer = { _referer }

  //客户端代理信息
  private[this] var _userAgent: String = _
  def setUserAgent (value: String): Unit = { _userAgent = value }
  def getUserAgent = { _userAgent }

  //跳转过来页面的域名:HTTP.HOST:request.referer.host
  private[this] var _referDomain: String = _
  def setReferDomain (value: String): Unit = { _referDomain = value }
  def getReferDomain = { _referDomain }
}

object ClickLogBean{
  //定义点击流日志解析规则
  val getLogFormat: String = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""

  //解析字符串转换成对象
  def apply(clickLog:String): ClickLogBean ={
    val parser = new HttpdLoglineParser[ClickLogBean](classOf[ClickLogBean], getLogFormat)
    val clickLogBean = new ClickLogBean
    parser.parse(clickLogBean, clickLog)
    clickLogBean
  }

  //创建点击流日志解析规则
  def createClickLogParser() ={
    // 定义一个解析器
    val parser = new HttpdLoglineParser[ClickLogBean](classOf[ClickLogBean], getLogFormat)
    // 定义一个别名.
    parser.addTypeRemapping("request.firstline.uri.query.g", "HTTP.URI")
    parser.addTypeRemapping("request.firstline.uri.query.r", "HTTP.URI")

    parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user")
    parser.addParseTarget("setIp", "IP:connection.client.host")
    parser.addParseTarget("setRequestTime", "TIME.STAMP:request.receive.time")
    parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.method")
    parser.addParseTarget("setResolution", "HTTP.URI:request.firstline.uri")
    parser.addParseTarget("setRequestProtocol", "HTTP.PROTOCOL_VERSION:request.firstline.protocol")
    parser.addParseTarget("setResponseBodyBytes", "BYTES:response.body.bytes")
    parser.addParseTarget("setReferer", "HTTP.URI:request.referer")
    parser.addParseTarget("setUserAgent", "HTTP.USERAGENT:request.user-agent")
    parser.addParseTarget("setReferDomain", "HTTP.HOST:request.referer.host")

    //返回点击流日志解析规则
    parser
  }

  def main(args: Array[String]): Unit = {
    val logline = "2001:980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\""

    val record = new ClickLogBean()
    val parser = createClickLogParser()
    parser.parse(record, logline)
    println(record.getConnectionClientUser)
    println(record.getIp)
    println(record.getRequestTime)
    println(record.getMethod)
    println(record.getResolution)
    println(record.getRequestProtocol)
    println(record.getResponseBodyBytes)
    println(record.getReferer)
    println(record.getUserAgent)
    println(record.getReferDomain)
  }
}

case class ClickLogWideBean(@BeanProperty uid:String,            //用户id信息
                            @BeanProperty ip:String,             //ip地址
                            @BeanProperty requestTime:String,    //请求时间
                            @BeanProperty requestMethod:String,  //请求方式
                            @BeanProperty requestUrl:String,     //请求地址
                            @BeanProperty requestProtocol:String, //请求协议
                            @BeanProperty responseStatus:Int,      //响应码
                            @BeanProperty responseBodyBytes:String,//返回的数据流量
                            @BeanProperty referrer:String,        //访客的来源url
                            @BeanProperty userAgent:String,       //客户端代理信息
                            @BeanProperty referDomain: String,    //跳转过来页面的域名:HTTP.HOST:request.referer.host
                            @BeanProperty var province: String,   //ip所对应的省份
                            @BeanProperty var city: String,       //ip所对应的城市
                            @BeanProperty var timestamp:Long      //时间戳
                           )

object ClickLogWideBean {
  def apply(clickLogBean: ClickLogBean): ClickLogWideBean = {
    ClickLogWideBean(
      clickLogBean.getConnectionClientUser,
      clickLogBean.getIp,
      "",
      //DateUtil.datetime2date(clickLogBean.getRequestTime),
      clickLogBean.getMethod,
      clickLogBean.getResolution,
      clickLogBean.getRequestProtocol,
      clickLogBean.getResponseStatus,
      clickLogBean.getResponseBodyBytes,
      clickLogBean.getReferer,
      clickLogBean.getUserAgent,
      clickLogBean.getReferDomain,
      "",
      "",
      0)
  }
}