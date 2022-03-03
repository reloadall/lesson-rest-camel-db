package org.example.entity;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.UUID;

@Getter
@Setter
public class Payment implements Serializable {
    private static final long serialVersionUID = -5303701097351627299L;

    private UUID id;
    private int amount;
    private String sender;
    private String recipient;
}
