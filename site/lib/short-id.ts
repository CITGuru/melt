// 10-char URL-safe short-id. nanoid alphabet excludes `_` and `-` to
// keep the IDs trivially copy-pasteable from terminals and chat apps
// where leading dashes can collide with flag parsing.
import { customAlphabet } from "nanoid";

const ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const generate = customAlphabet(ALPHABET, 10);

export function newShortId(): string {
  return generate();
}

const SHORT_ID_RE = /^[0-9A-Za-z]{10}$/;
export function isValidShortId(id: string): boolean {
  return SHORT_ID_RE.test(id);
}
