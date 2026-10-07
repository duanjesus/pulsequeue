package com.pulsequeue.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Guards the write endpoints only — publishing events (POST /api/v1/events*)
 * and changing webhook subscriptions (any non-GET on /api/v1/webhooks*) —
 * with a shared-secret header: "only trusted internal services can publish"
 * for a demo, not a real production auth scheme. Every other endpoint
 * (dashboard reads, actuator, swagger) passes through untouched: this is
 * deliberately narrow, not a login wall in front of the whole app.
 */
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    public static final String API_KEY_HEADER = "X-API-Key";

    private final ApiKeyProperties properties;
    private final ObjectMapper objectMapper;

    public ApiKeyAuthFilter(ApiKeyProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws jakarta.servlet.ServletException, IOException {

        if (!isProtected(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        String providedKey = request.getHeader(API_KEY_HEADER);
        if (providedKey == null || !providedKey.equals(properties.apiKey())) {
            writeUnauthorized(response);
            return;
        }

        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("producer", null, List.of(new SimpleGrantedAuthority("ROLE_PRODUCER"))));
        filterChain.doFilter(request, response);
    }

    private boolean isProtected(HttpServletRequest request) {
        String method = request.getMethod();
        String uri = request.getRequestURI();
        boolean publishesEvents = "POST".equalsIgnoreCase(method) && uri.startsWith("/api/v1/events");
        boolean changesWebhooks = !"GET".equalsIgnoreCase(method) && uri.startsWith("/api/v1/webhooks");
        return publishesEvents || changesWebhooks;
    }

    private void writeUnauthorized(HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", HttpStatus.UNAUTHORIZED.value());
        body.put("error", HttpStatus.UNAUTHORIZED.getReasonPhrase());
        body.put("message", "Missing or invalid " + API_KEY_HEADER + " header");
        response.getWriter().write(objectMapper.writeValueAsString(body));
    }
}
