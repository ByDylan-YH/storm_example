package project.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PageUtil {
    private static Logger logger = LoggerFactory.getLogger(PageUtil.class);
    private static HttpClientBuilder builder = HttpClients.custom();

    public static String getContent(String url) {
//        设置浏览器的信息
        builder.setUserAgent("Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.132 Safari/537.36");
//        这个代理ip不能写死,需要从代理ip库中动态获取
        String proxy_ip = "121.40.78.138";
        int proxy_port = 3128;
        HttpHost proxy = new HttpHost(proxy_ip, proxy_port);
        CloseableHttpClient client = builder.setProxy(proxy).build();
        String content = null;
        long start_time = System.currentTimeMillis();

        HttpGet request = new HttpGet(url);
        try {
            CloseableHttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();
            content = EntityUtils.toString(entity);
//            logger.info(Thread.currentThread().getId()+"页面下载成功,url是：{},耗时：{}",url,System.currentTimeMillis()-start_time);
            client.close();
        } catch (HttpHostConnectException e) {
            logger.error("代理IP失效：ip:{},port:{},失败的url:{}", proxy_ip, proxy_port, url);
        } catch (Exception e) {
            logger.error("页面下载失败,url是：{}", url);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return content;
    }

    public static void main(String[] args) {
        String content = getContent("https://freeapi.ipip.net/113.45.32.253");
        System.out.println(content);
    }
}
