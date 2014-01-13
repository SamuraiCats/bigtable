package com.altamiracorp.bigtableui.util;

import com.altamiracorp.miniweb.Handler;
import com.altamiracorp.miniweb.HandlerChain;
import org.apache.commons.io.FileUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;

public class SimpleTemplateFileHandler implements Handler {
    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
        String path = request.getServletContext().getRealPath(request.getPathInfo());
        String contents = FileUtils.readFileToString(new File(path));

        String url = request.getRequestURL().toString();
        int slashIndex = url.indexOf('/', "https://".length() + 1);
        String baseUrl = url.substring(0, slashIndex);

        contents = contents.replaceAll("\\$\\{context.url}", baseUrl + request.getServletContext().getContextPath() + "/");

        response.setContentType("text/html");
        response.getOutputStream().write(contents.getBytes());
    }
}
