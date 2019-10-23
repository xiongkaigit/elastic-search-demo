/**
 * Author:   xiongkai
 * Date:     2019-09-06 11:38
 */
package com.example.elasticsearch.controller;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearch.entity.Animal;
import com.example.elasticsearch.entity.query.AnimalQuery;
import com.example.elasticsearch.entity.vo.AnimalVO;
import com.example.elasticsearch.util.UUIDUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/elasticsearch")
@Api(value="elasticsearch",tags={"elasticsearch接口"})
@Slf4j
public class ElasticSearchController {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    @ApiOperation(value = "save record" ,  notes="save record")
    @PostMapping(value = "saveRecord")
    void saveRecord(@RequestBody Animal animal) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", animal.getName());
        jsonMap.put("description", animal.getDescription());
        jsonMap.put("createDate", new Date());

        //IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(null, IndexAction.INSTANCE, message.getIndexName());
        /**
         * 自定义uuid作为文档id
         */
        IndexRequest indexRequest = new IndexRequest(animal.getIndexName()).id(UUIDUtils.getUUID()).source(jsonMap);
        /**
         * 不指定id，es自动生成
         */
        //IndexRequest indexRequest = new IndexRequest(animal.getIndexName()).source(jsonMap);
        /**
         * elasticsearch 7.0以上版本已经删除type
         */
        //IndexRequest indexRequest = new IndexRequest(message.getIndexName(), message.getType(), UUIDUtils.getUUID()).source(jsonMap);
        //IndexRequest indexRequest = new IndexRequest(message.getIndexName()).type(message.getType()).id(message.getId()).source(jsonMap);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            log.info(response.toString());
        } catch (ElasticsearchException e) {
            log.error("ElasticSearchController-createIndex-ElasticsearchException", e);
            if (e.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            log.error("ElasticSearchController-createIndex-IOException", e);
        }
    }

    @ApiOperation(value = "select record" ,  notes="select record")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name="index", value="索引", dataType="string", paramType="query"),
            @ApiImplicitParam(name="id", value="id", dataType="string", paramType="query"),
            @ApiImplicitParam(name="name", value="name", dataType="string", paramType="query")
    })
    @GetMapping(value = "selRecord")
    AnimalVO selRecord(String index, String id, String name) {
        AnimalVO animalVO = null;
        try {
            GetRequest getRequest = new GetRequest(index, id);
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            log.info("getResponse: {}", getResponse);
            if (getResponse.isExists() && !getResponse.isSourceEmpty()) {
                String source = getResponse.getSourceAsString();
                animalVO = JSON.parseObject(source, AnimalVO.class);
            }

            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("name", name));
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(5);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            log.info("searchResponse: {}", searchResponse);
            if (searchResponse != null) {

            }
        } catch (Exception e) {
            log.error("ElasticSearchController-selRecord-Exception", e);
        }
        return animalVO;
    }

    @ApiOperation(value = "update record" ,  notes="update record")
    @PutMapping(value = "updateRecord")
    void updateRecord(@RequestBody AnimalQuery animalQuery) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "Donald Duck");
        jsonMap.put("description", "duck");
        jsonMap.put("createDate", new Date());
        IndexRequest indexRequest = new IndexRequest(animalQuery.getIndex()).id(animalQuery.getId()).source(jsonMap);
        try {
            UpdateRequest updateRequest = new UpdateRequest(animalQuery.getIndex(), animalQuery.getId()).doc(indexRequest);
            restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("ElasticSearchController-updateRecord-Exception", e);
        }
    }


    @ApiOperation(value = "delete record" ,  notes="delete record")
    @DeleteMapping(value = "delRecord")
    void delRecord(@RequestBody AnimalQuery animalQuery) {
        DeleteRequest deleteRequest = new DeleteRequest(animalQuery.getIndex(), animalQuery.getId());
        try {
            restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("ElasticSearchController-delRecord-Exception", e);
        }
    }


}
