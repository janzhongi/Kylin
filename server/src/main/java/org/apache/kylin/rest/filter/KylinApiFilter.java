package org.apache.kylin.rest.filter;

import com.github.isrsal.logging.RequestWrapper;
import com.github.isrsal.logging.ResponseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jiazhong on 2015/4/20.
 */

public class KylinApiFilter extends OncePerRequestFilter {
    protected static final Logger logger = LoggerFactory.getLogger(KylinApiFilter.class);
    private static final String REQUEST_PREFIX = "Request: ";
    private static final String RESPONSE_PREFIX = "Response: ";
    private AtomicLong id = new AtomicLong(1L);

    public KylinApiFilter() {
    }

    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        if(logger.isDebugEnabled()) {
            long requestId = this.id.incrementAndGet();
            request = new RequestWrapper(Long.valueOf(requestId), (HttpServletRequest)request);
            response = new ResponseWrapper(Long.valueOf(requestId), (HttpServletResponse)response);
        }

        try {
            filterChain.doFilter((ServletRequest)request, (ServletResponse)response);
        } finally {
            if(logger.isDebugEnabled()) {
                this.logRequest((HttpServletRequest)request);
//                this.logResponse((ResponseWrapper)response);
            }

        }

    }

    private void logRequest(HttpServletRequest request) {
        StringBuilder msg = new StringBuilder();
        msg.append("Request: ");
        HttpSession session = request.getSession(true);
        SecurityContext context = (SecurityContext)session.getAttribute("SPRING_SECURITY_CONTEXT");

        String requester="";
        if(context!=null){
            Authentication authentication=  context.getAuthentication();
            if(authentication!=null){
                requester = authentication.getName();
            }

        }
        msg.append("requester= "+requester).append(";");

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        msg.append("request time= " + format.format(new Date())).append(";");
        msg.append("uri=").append(request.getRequestURI());
        msg.append('?').append(request.getQueryString());
        if(request instanceof RequestWrapper && !this.isMultipart(request)) {
            RequestWrapper requestWrapper = (RequestWrapper)request;

            try {
                String e = requestWrapper.getCharacterEncoding() != null?requestWrapper.getCharacterEncoding():"UTF-8";
                msg.append("; payload=").append(new String(requestWrapper.toByteArray(), e));
            } catch (UnsupportedEncodingException var6) {
                logger.warn("Failed to parse request payload", var6);
            }
        }

        logger.debug(msg.toString());
    }

    private boolean isMultipart(HttpServletRequest request) {
        return request.getContentType() != null && request.getContentType().startsWith("multipart/form-data");
    }

    private void logResponse(ResponseWrapper response) {
        StringBuilder msg = new StringBuilder();
        msg.append("Response: ");
        msg.append("request id=").append(response.getId());

        try {
            msg.append("; payload=").append(new String(response.toByteArray(), response.getCharacterEncoding()));
        } catch (UnsupportedEncodingException var4) {
            logger.warn("Failed to parse response payload", var4);
        }

        logger.debug(msg.toString());
    }
}
