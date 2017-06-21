package com.dotcms.hazelcast.mapstore;





	public class Fqn {
		final String group, key, id;

		public Fqn(String group, String key) {
			if (group == null) {
				throw new RuntimeException("cache group is null");
			}
			if (key == null) {
				throw new RuntimeException("cache key is null");
			}
			this.group = group;
			this.key = key;

			this.id = betterHash(this.group.toLowerCase() + " | " + this.key.toLowerCase());
		}

		public Fqn(String group) {
			this(group, "");
		}

		@Override
		public String toString() {
			return (group + " | " + key);
		}

		@Override
		public boolean equals(Object obj) {

			return id.equals(((Fqn) obj).id);
		}

		private String betterHash(String s) {
			long h = 1125899906842597L; // prime
			int len = s.length();

			for (int i = 0; i < len; i++) {
				h = 31 * h + s.charAt(i);
			}

			return String.valueOf(h);
		}
}
