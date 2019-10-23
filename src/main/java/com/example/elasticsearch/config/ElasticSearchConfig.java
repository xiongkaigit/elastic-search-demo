/**
 * Author:   xiongkai
 * Date:     2019-09-05 16:20
 */
package com.example.elasticsearch.config;

import lombok.Data;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import sun.java2d.DisposerRecord;

import java.io.IOException;

@Component
@Configuration
@PropertySource(value="classpath:/config/elasticsearch.properties")
@ConfigurationProperties( prefix="es")
@Slf4j
@Data
public class ElasticSearchConfig implements DisposerRecord {

    private String hostName;

    private Integer transport;

    private String clusterName;

    private RestHighLevelClient highLevelClient;

    private final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    @Bean
    public RestHighLevelClient getRestHighLevelClient() {
        //System.out.println(hostName+","+transport+","+clusterName);
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(clusterName, ""));
        highLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostName, transport, "http"))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                            //这里可以设置一些参数，比如cookie存储、代理等等
                            httpAsyncClientBuilder.disableAuthCaching();
                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        })
        );
        return highLevelClient;
    }

    @Override
    public void dispose() {
        if (highLevelClient != null) {
            try {
                log.info("Closing elasticSearch client");
                highLevelClient.close();
            } catch (IOException e) {
                log.error("Error closing ElasticSearch client: ", e);
                e.printStackTrace();
            }
        }
    }

}
