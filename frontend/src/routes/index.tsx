import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
  component: HomeComponent,
});

function HomeComponent() {
  return (
    <div className="p-2">
      <div className="py-24 px-16 w-full">
        <form
          action=""
          className="max-w-xs mx-auto flex flex-col space-y-4 [&>input]:px-4 [&>input]:py-2 [&>input]:border-cyan-800 [&>input]:border"
        >
          <input type="text" placeholder="John Doe" name="name" />
          <input type="email" placeholder="Email" name="email" />
          <button type="submit" className="px-3 py-1.5 bg-gray-300">
            Submit
          </button>
        </form>
      </div>
    </div>
  );
}
