package org.cscie88c.week7


final case class CustomerTransaction(
    customerId: String,
    transactionDate: String,
    transactionAmount: Double
)

object CustomerTransaction {
  // add companion object methods below
}
