variable "name" { type = string }

variable "secret_keys" {
  description = "Secret KEY NAMES to create empty placeholders for."
  type        = list(string)
}
