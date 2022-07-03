package com.pgsqlproject2.pgproj2.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class JDBCController {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @GetMapping("/centerList")
    public List<Map<String, Object>> centerList(){
        String sql = "select * from center";
        List<Map<String, Object>> ret = jdbcTemplate.queryForList(sql);
        return ret;
    }
}
