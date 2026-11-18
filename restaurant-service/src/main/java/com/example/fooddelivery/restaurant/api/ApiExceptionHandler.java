package com.example.fooddelivery.restaurant.api;

import com.example.fooddelivery.restaurant.domain.TicketService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** RFC 7807 responses. The base class renders Spring MVC's own errors (400 bodies, ResponseStatusException) too. */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(TicketService.TicketNotFoundException.class)
    public ProblemDetail notFound(TicketService.TicketNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(TicketService.TicketConflictException.class)
    public ProblemDetail conflict(TicketService.TicketConflictException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }
}
