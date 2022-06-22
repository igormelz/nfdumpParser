package ru.openfs.nfdump;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv4.IPv4AddressAssociativeTrie;
import inet.ipaddr.ipv4.IPv4AddressAssociativeTrie.IPv4AssociativeTrieNode;
import io.quarkus.logging.Log;

@Singleton
public class IpNetworks {
    IPv4AddressAssociativeTrie<Subnet> trie = new IPv4AddressAssociativeTrie<Subnet>();

    @ConfigProperty(name = "ipdat")
    URL mapUrl;

    @PostConstruct
    void init() {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(mapUrl.openStream(), Charset.forName("UTF-8")))) {
            br.lines().map(line -> line.split(";")).filter(st -> st.length == 2).forEach(st -> {
                addToNets(trie, st[1], st[0]);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        Log.info("Loaded");
    }

    public String find(int ipaddr) {
        IPv4Address addr = new IPv4Address(ipaddr);
        IPv4AssociativeTrieNode<Subnet> subnetNode = trie.longestPrefixMatchNode(addr);
        return subnetNode != null ? subnetNode.getValue().toString() : null;
    }

    static void addToNets(IPv4AddressAssociativeTrie<Subnet> nets, String subnet, String name) {
        nets.put(new IPAddressString(subnet).getAddress().toIPv4(), new Subnet(name));
    }

    static class Subnet {
        String name;

        public Subnet(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
