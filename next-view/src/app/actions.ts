"use server";

import { z } from "zod";

// Define the user schema for type safety
const UserSchema = z.object({
  _id: z.string(),
  did: z.string(),
  handle: z.string(),
  displayName: z.string(),
  avatar: z.string().optional(),
  associated: z.record(z.any()).optional(),
  labels: z.array(z.any()).optional(),
  createdAt: z.string(),
  description: z.string().optional(),
  indexedAt: z.string().optional(),
  banner: z.string().optional(),
  followersCount: z.number(),
  followsCount: z.number(),
  postsCount: z.number(),
  sfc: z.number().optional(),
  posts: z.array(z.any()).optional(),
  metadata: z
    .object({
      topics_of_expertise: z.array(z.string()).optional(),
    })
    .optional(),
});

export type User = z.infer<typeof UserSchema>;

// Define valid sort fields as keys of the User type
type UserKey = keyof User;
const validSortFields = [
  "followersCount",
  "followsCount",
  "postsCount",
  "createdAt",
  "_id",
  "handle",
  "displayName",
] as const;
export type ValidSortField = (typeof validSortFields)[number];

// Define the search params schema
const SearchParamsSchema = z.object({
  query: z.string().optional(),
  page: z.coerce.number().default(1),
  limit: z.coerce.number().default(10),
  sortField: z.enum(validSortFields).default("followersCount"),
  sortDirection: z.enum(["asc", "desc"]).default("desc"),
  minFollowers: z.coerce.number().optional(),
  maxFollowers: z.coerce.number().optional(),
  minPosts: z.coerce.number().optional(),
  maxPosts: z.coerce.number().optional(),
});

export type SearchParams = z.infer<typeof SearchParamsSchema>;

// Mock data - in a real app, this would be a database query
const mockUsers = [
  {
    _id: "67d0ddcea2ca26d3737740df",
    did: "did:plc:e62gb2ushvtvjvqcbrxeaw2n",
    handle: "chrislhayes.bsky.social",
    displayName: "Chris Hayes",
    avatar: "/placeholder.svg?height=40&width=40",
    associated: {},
    labels: [],
    createdAt: "2023-04-29T01:20:57.120Z",
    description:
      "Bronx boy. Cubs fan. Dad, husband, writer, podcaster and cable news host.",
    indexedAt: "2025-01-31T02:28:23.944Z",
    banner: "/placeholder.svg?height=100&width=400",
    followersCount: 655383,
    followsCount: 404,
    postsCount: 2328,
    sfc: 243,
    posts: Array(10).fill({}),
    metadata: {
      topics_of_expertise: ["politics", "media", "current events"],
    },
  },
  {
    _id: "67d0ddcea2ca26d3737740e0",
    did: "did:plc:f62gb2ushvtvjvqcbrxeaw3m",
    handle: "ezraklein.bsky.social",
    displayName: "Ezra Klein",
    avatar: "/placeholder.svg?height=40&width=40",
    associated: {},
    labels: [],
    createdAt: "2023-05-15T14:30:22.120Z",
    description:
      "NY Times columnist. Host of The Ezra Klein Show podcast. Co-founder of Vox.",
    indexedAt: "2025-02-01T12:18:43.944Z",
    banner: "/placeholder.svg?height=100&width=400",
    followersCount: 542189,
    followsCount: 312,
    postsCount: 1856,
    sfc: 198,
    posts: Array(10).fill({}),
    metadata: {
      topics_of_expertise: ["politics", "economics", "media"],
    },
  },
  {
    _id: "67d0ddcea2ca26d3737740e1",
    did: "did:plc:g62gb2ushvtvjvqcbrxeaw4p",
    handle: "kaitlintiffany.bsky.social",
    displayName: "Kaitlyn Tiffany",
    avatar: "/placeholder.svg?height=40&width=40",
    associated: {},
    labels: [],
    createdAt: "2023-06-02T09:45:12.120Z",
    description:
      "Staff writer at The Atlantic. Author of 'Everything I Need I Get From You'.",
    indexedAt: "2025-01-28T18:42:11.944Z",
    banner: "/placeholder.svg?height=100&width=400",
    followersCount: 128456,
    followsCount: 523,
    postsCount: 1243,
    sfc: 87,
    posts: Array(10).fill({}),
    metadata: {
      topics_of_expertise: ["internet culture", "technology", "fandom"],
    },
  },
  {
    _id: "67d0ddcea2ca26d3737740e2",
    did: "did:plc:h62gb2ushvtvjvqcbrxeaw5q",
    handle: "taylorlorenz.bsky.social",
    displayName: "Taylor Lorenz",
    avatar: "/placeholder.svg?height=40&width=40",
    associated: {},
    labels: [],
    createdAt: "2023-04-18T11:22:33.120Z",
    description:
      "Technology columnist at The Washington Post covering internet culture and online communities.",
    indexedAt: "2025-02-02T09:15:37.944Z",
    banner: "/placeholder.svg?height=100&width=400",
    followersCount: 389742,
    followsCount: 1205,
    postsCount: 3421,
    sfc: 176,
    posts: Array(10).fill({}),
    metadata: {
      topics_of_expertise: ["internet culture", "social media", "technology"],
    },
  },
  {
    _id: "67d0ddcea2ca26d3737740e3",
    did: "did:plc:i62gb2ushvtvjvqcbrxeaw6r",
    handle: "caseynewton.bsky.social",
    displayName: "Casey Newton",
    avatar: "/placeholder.svg?height=40&width=40",
    associated: {},
    labels: [],
    createdAt: "2023-05-05T16:40:19.120Z",
    description:
      "Writer of Platformer, a newsletter about Big Tech and democracy.",
    indexedAt: "2025-01-30T14:22:51.944Z",
    banner: "/placeholder.svg?height=100&width=400",
    followersCount: 275631,
    followsCount: 892,
    postsCount: 2754,
    sfc: 132,
    posts: Array(10).fill({}),
    metadata: {
      topics_of_expertise: ["technology", "social media", "platforms"],
    },
  },
];

export async function searchUsers(params: SearchParams) {
  // Parse and validate the search params
  const validParams = SearchParamsSchema.parse(params);

  // In a real app, this would be a database query or API call to a search service
  // For example with a full-text search service like Elasticsearch, Algolia, or Typesense

  // Simulate server processing delay
  await new Promise((resolve) => setTimeout(resolve, 500));

  let filteredUsers = [...mockUsers];

  // Apply search query (simulating full-text search)
  if (validParams.query) {
    const query = validParams.query.toLowerCase();
    filteredUsers = filteredUsers.filter(
      (user) =>
        user.handle.toLowerCase().includes(query) ||
        user.displayName.toLowerCase().includes(query) ||
        user._id.toLowerCase().includes(query) ||
        user.did.toLowerCase().includes(query) ||
        (user.description && user.description.toLowerCase().includes(query)),
    );
  }

  // Apply filters
  if (validParams.minFollowers) {
    filteredUsers = filteredUsers.filter(
      (user) => user.followersCount >= validParams.minFollowers!,
    );
  }
  if (validParams.maxFollowers) {
    filteredUsers = filteredUsers.filter(
      (user) => user.followersCount <= validParams.maxFollowers!,
    );
  }
  if (validParams.minPosts) {
    filteredUsers = filteredUsers.filter(
      (user) => user.postsCount >= validParams.minPosts!,
    );
  }
  if (validParams.maxPosts) {
    filteredUsers = filteredUsers.filter(
      (user) => user.postsCount <= validParams.maxPosts!,
    );
  }

  // Get total count before pagination
  const totalCount = filteredUsers.length;

  // Apply sorting
  filteredUsers.sort((a, b) => {
    const field = validParams.sortField as keyof User;
    if (validParams.sortDirection === "asc") {
      return a[field] > b[field] ? 1 : -1;
    } else {
      return a[field] < b[field] ? 1 : -1;
    }
  });

  // Apply pagination
  const startIndex = (validParams.page - 1) * validParams.limit;
  const paginatedUsers = filteredUsers.slice(
    startIndex,
    startIndex + validParams.limit,
  );

  return {
    users: paginatedUsers,
    totalCount,
    page: validParams.page,
    limit: validParams.limit,
    totalPages: Math.ceil(totalCount / validParams.limit),
  };
}

export async function getUserById(id: string) {
  // In a real app, this would be a database query
  const user = mockUsers.find((user) => user._id === id);

  if (!user) {
    throw new Error("User not found");
  }

  return user;
}
