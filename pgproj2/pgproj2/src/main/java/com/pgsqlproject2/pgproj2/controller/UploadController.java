package com.pgsqlproject2.pgproj2.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

@Controller
@RequestMapping("thymeleaf")
public class UploadController {
    @Autowired
    private HttpServletRequest request;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping(value = "upload", method = RequestMethod.GET)
    public String load() {
        return "upload";
    }

    @RequestMapping(value = "uploadBlog", method = RequestMethod.POST)
    @ResponseBody
    public String uploadBlog(MultipartFile file114) {
        String originalFilename = file114.getOriginalFilename();
        assert originalFilename != null;
        if(originalFilename.equals("stockIn.csv")){
            jdbcTemplate.execute("truncate table \"stockIn\";");
            String sql = "copy \"stockIn\" from 'D:\\dbCode\\project2\\stockIn.csv' with csv header delimiter ',';\n";
            jdbcTemplate.execute(sql);
            return """
                    <body style="text-align: center; font-family:consolas; padding:20px;">
                        finished
                    </body>""";
        }
        else if(originalFilename.equals("placeOrder.csv")){
            jdbcTemplate.execute("truncate table \"placeOrder\";");
            String sql = "copy \"placeOrder\" from 'D:\\dbCode\\project2\\placeOrder.csv' with csv header delimiter ',';\n";
            jdbcTemplate.execute(sql);
            return """
                    <body style="text-align: center; font-family:consolas; padding:20px;">
                        finished
                    </body>""";
        }
        else return """
                    <body style="text-align: center; font-family:consolas; padding:20px;">
                        please select valid file!
                    </body>""";
    }
}
