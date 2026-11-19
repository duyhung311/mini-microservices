# 0008. Java 17 instead of 21

## Status
Accepted (binding user decision). It deviates from spec section 12.

## Context
The spec asks for Java 21. The user's machine and toolchain use JDK 17.

## Decision
- `maven.compiler.release=17`. The base image is `eclipse-temurin:17.0.21_9-jre`.
- Spring Boot 3.5.16 and Spring Cloud 2025.0.3 both support Java 17.
- Records, sealed interfaces and `instanceof` patterns are allowed. Pattern-matching `switch` is not, because it is final only in Java 21.
- Virtual threads are not used.

## Consequences
- Code in the repo compiles on 17 and on 21.
- `EventPayload` is a sealed interface. Code avoids pattern-matching `switch`.
- Moving to 21 later means changing the release property, the Docker base image and the CI JDK.
