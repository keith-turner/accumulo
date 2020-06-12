package org.apache.accumulo.core.data;

/**
 * @since 2.1.0
 */
public final class UserCompactionId extends AbstractId<UserCompactionId> {

  private static final long serialVersionUID = 1L;

  private UserCompactionId(String canonical) {
    super(canonical);
  }

  /**
   * Get a TableId object for the provided canonical string. This is guaranteed to be non-null.
   *
   * @param canonical
   *          table ID string
   * @return Table.ID object
   */
  public static UserCompactionId of(final String canonical) {
    return new UserCompactionId(canonical);
  }

}
