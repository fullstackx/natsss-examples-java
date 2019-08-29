// Copyright 2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.liwei.fullstack.nats.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;

import io.nats.client.AuthHandler;
import io.nats.client.NKey;

public class ExampleAuthHandler implements AuthHandler {
    private NKey nkey;

    public ExampleAuthHandler(String path) throws Exception {
        BufferedReader in = null;

        try {
            in = new BufferedReader((new FileReader(new File(path))));

            char[] buffer = new char[2048];
            int len = in.read(buffer);
            char[] seed = new char[len];

            System.arraycopy(buffer, 0, seed, 0, len);

            this.nkey = NKey.fromSeed(seed);

            for (int i=0;i<buffer.length;i++) {
                buffer[i] = '\0';
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    public NKey getNKey() {
        return this.nkey;
    }

    public char[] getID() {
        try {
            return this.nkey.getPublicKey();
        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
            return null;
        }
    }

    public byte[] sign(byte[] nonce) {
        try {
            return this.nkey.sign(nonce);
        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
            return null;
        }
    }

    public char[] getJWT() {
        return null;
    }
}